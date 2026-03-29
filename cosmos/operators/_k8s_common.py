"""Shared helpers for Kubernetes and GCP GKE dbt operators.

Both ``cosmos.operators.kubernetes`` and ``cosmos.operators.gcp_gke`` (and their
watcher variants) implement near-identical logic parameterised only by the
Airflow pod operator class (``KubernetesPodOperator`` vs ``GKEStartPodOperator``).
This module extracts that shared logic so it lives in a single place.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from os import PathLike
from typing import TYPE_CHECKING, Any

import kubernetes.client as k8s
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.callbacks import client_type
from airflow.utils.context import context_merge

from cosmos.log import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pendulum import DateTime

try:
    # apache-airflow-providers-cncf-kubernetes >= 7.14.0
    from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
except ImportError:

    class KubernetesPodOperatorCallback:  # type: ignore[no-redef]
        """Mock fallback for older versions. Should not be used in practice."""

        pass


from cosmos.dbt.parser.output import extract_log_issues
from cosmos.operators._watcher.base import store_dbt_resource_status_from_log
from cosmos.operators.base import AbstractDbtBase

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2

logger = get_logger(__name__)


def init_k8s_operator(
    operator: Any,
    pod_operator_class: type,
    profile_config: Any,
    kwargs: dict[str, Any],
) -> None:
    """Shared ``__init__`` logic for K8s-based dbt base operators.

    Segregates kwargs between ``AbstractDbtBase`` and the pod operator class, then
    initializes both explicitly (no ``super().__init__()`` to avoid MRO conflicts).
    """
    operator.profile_config = profile_config

    # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
    # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
    # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
    # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
    # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
    # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
    default_args = kwargs.get("default_args", {})
    operator_kwargs: dict[str, Any] = {}
    operator_args: set[str] = set()
    for clazz in pod_operator_class.__mro__:
        operator_args.update(inspect.signature(clazz.__init__).parameters.keys())
        if clazz == BaseOperator:
            break
    for arg in operator_args:
        try:
            operator_kwargs[arg] = kwargs[arg]
        except KeyError:
            pass

    base_kwargs: dict[str, Any] = {}
    for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
        try:
            base_kwargs[arg] = kwargs[arg]
        except KeyError:
            try:
                base_kwargs[arg] = default_args[arg]
            except KeyError:
                pass

    AbstractDbtBase.__init__(operator, **base_kwargs)

    container_resources = operator_kwargs.get("container_resources")
    if isinstance(container_resources, dict):
        operator_kwargs["container_resources"] = k8s.V1ResourceRequirements(**container_resources)
    pod_operator_class.__init__(operator, **operator_kwargs)


def _build_env_vars(
    env: dict[str, str | bytes | PathLike[Any]],
    existing_env_vars: list[Any],
) -> list[Any]:
    """Merge an env dict with existing K8s env vars and return the combined list."""
    env_vars_dict = {k: str(v) for k, v in env.items()}
    for ev in existing_env_vars:
        env_vars_dict[ev.name] = ev.value
    return convert_env_vars(env_vars_dict)


def build_kube_args(operator: Any, context: Any, cmd_flags: list[str] | None = None) -> None:
    """Build the dbt command, set env vars, and assign ``cmds``/``arguments`` on the operator.

    Always splits the executable from arguments (``self.cmds = ["dbt"]``, ``self.arguments = [...]``)
    to handle container images with or without ``ENTRYPOINT ["dbt"]``.
    """
    # For the first round, we're going to assume that the command is dbt
    # This means that we don't have openlineage support, but we will create a ticket
    # to add that in the future
    operator.dbt_executable_path = "dbt"
    dbt_cmd, env_vars = operator.build_cmd(context=context, cmd_flags=cmd_flags)

    # Parse ProfileConfig and add additional arguments to the dbt_cmd
    if operator.profile_config:
        if operator.profile_config.profile_name:
            dbt_cmd.extend(["--profile", operator.profile_config.profile_name])
        if operator.profile_config.target_name:
            dbt_cmd.extend(["--target", operator.profile_config.target_name])

    if operator.project_dir:
        dbt_cmd.extend(["--project-dir", str(operator.project_dir)])

    operator.env_vars = _build_env_vars(env_vars, operator.env_vars)

    # Split the executable from arguments to avoid double invocation when the
    # container image has ENTRYPOINT ["dbt"]. Setting self.cmds overrides the
    # image's ENTRYPOINT, so this works regardless of image configuration.
    operator.cmds = [dbt_cmd[0]]
    operator.arguments = dbt_cmd[1:]


def build_and_run_cmd(
    operator: Any,
    pod_operator_class: type,
    context: Any,
    cmd_flags: list[str] | None = None,
) -> Any:
    """Build kube args, log the command, and invoke the pod operator's ``execute``."""
    operator.invoke_interceptors(context)
    operator.build_kube_args(context, cmd_flags)
    operator.log.info(f"Running command: {operator.arguments}")
    result = pod_operator_class.execute(operator, context)
    operator.log.info(result)


# Pre-compiled regex patterns for warning detection (constant, compiled once)
#
# Warning count pattern
# Matches: "Done. PASS=X WARN=Y ERROR=Z SKIP=W"
_WARN_COUNT_PATTERN = re.compile(r"Done\. (?:\w+=\d+ )*WARN=(\d+)(?: \w+=\d+)*")
# Primary pattern for source freshness warnings
# Matches: "HH:MM:SS X of Y WARN freshness of source.table ... [WARN in Xs]"
_FRESHNESS_PATTERN = re.compile(
    r"(\d{2}:\d{2}:\d{2})\s+"
    r"\d+\s+of\s+\d+\s+"
    r"WARN\s+freshness\s+of\s+"
    r"([^\s]+)"
    r".*?\[WARN\s+in\s+([\d.]+)s\]"
)
# Secondary pattern for simpler source freshness warnings
# Matches: "WARN freshness of source_name"
_SIMPLE_FRESHNESS_PATTERN = re.compile(r"WARN\s+freshness\s+of\s+([^\s]+)")


class DbtTestWarningHandler(KubernetesPodOperatorCallback):
    """
    Detect dbt test and source freshness warnings from pod logs.

    This handler can detect warnings from:
    1. Regular dbt tests (using the standard "Done. PASS=X WARN=Y" pattern)
    2. Source freshness tests (using "WARN freshness of..." pattern)

    The ``test_operator_class`` and ``source_operator_class`` parameters allow
    the same handler to work for both Kubernetes and GCP GKE operator variants.
    """

    def __init__(
        self,
        on_warning_callback: Callable[..., Any],
        operator: Any,
        context: Any | None = None,
        test_operator_class: type | None = None,
        source_operator_class: type | None = None,
    ) -> None:
        self.on_warning_callback = on_warning_callback
        self.operator = operator
        self.context = context
        self.test_operator_class = test_operator_class
        self.source_operator_class = source_operator_class

    def on_pod_completion(
        self,
        *,
        pod: k8s.V1Pod,
        **kwargs: Any,
    ) -> None:
        """
        Handles warnings by extracting log issues, creating additional context, and calling the
        on_warning_callback with the updated context.

        Note that the signature of the interface method changed in `cncf.kubernetes` provider version 10.2.0, where the
        `operator` and `context` parameters were added to all operator callbacks. To maintain forward compatibility
        with `cncf.kubernetes` provider version 7.14.0 and later, we pass these parameters via instance variables.

        :param pod: the created pod.
        """
        if not self.context:
            self.operator.log.warning("No context provided to the DbtTestWarningHandler.")
            return

        task = self.context["task_instance"].task
        if not (isinstance(task, self.test_operator_class) or isinstance(task, self.source_operator_class)):
            self.operator.log.warning(f"Cannot handle dbt warnings for task of type {type(task)}.")
            return

        # Get the logs from the pod
        logs = []
        for log in task.pod_manager.read_pod_logs(pod, "base"):
            decoded_log = log.decode("utf-8")
            if decoded_log != "":
                logs.append(decoded_log)

        logs_text = "\n".join(logs)

        # Check for warnings
        warning_detected = False
        if isinstance(task, self.test_operator_class):
            warn_count = self._detect_standard_warnings(logs_text)
            if warn_count:
                self.operator.log.info(f"Detected {warn_count} warnings using standard pattern")
                warning_detected = True
        elif isinstance(task, self.source_operator_class):
            source_freshness_warnings = self._detect_source_freshness_warnings(logs_text)
            if source_freshness_warnings:
                self.operator.log.info(f"Detected {len(source_freshness_warnings)} source freshness warnings")
                warning_detected = True

        if not warning_detected:
            self.operator.log.warning(
                "Failed to scrape warning count from the pod logs. Potential warning callbacks could not be triggered."
            )
            return

        test_names, test_results = extract_log_issues(logs)
        context_merge(self.context, test_names=test_names, test_results=test_results)
        self.on_warning_callback(self.context)

    def _detect_standard_warnings(self, log_text: str) -> int | None:
        """
        Detect warnings using the standard dbt summary pattern.

        Pattern: "Done. PASS=X WARN=Y ERROR=Z SKIP=W"

        :param log_text: Complete log text from the pod
        :return: Number of warnings detected, or None if pattern not found
        """
        match = _WARN_COUNT_PATTERN.search(log_text)
        if match:
            return int(match.group(1))
        return None

    def _detect_source_freshness_warnings(self, log_text: str) -> list[dict[str, Any]]:
        """
        Detect source freshness warnings from dbt logs.

        Pattern examples:
        - "15:49:21 1 of 1 WARN freshness of auction_net.auction_net_raw ... [WARN in 0.90s]"
        - "WARN freshness of source_name.table_name"

        :param log_text: Complete log text from the pod
        :return: List of warning dictionaries
        """
        warnings: list[dict[str, Any]] = []

        for match in _FRESHNESS_PATTERN.finditer(log_text):
            timestamp = match.group(1)
            source_name = match.group(2)
            execution_time = match.group(3)

            warnings.append(
                {
                    "name": f"source_freshness_{source_name}",
                    "status": "WARN",
                    "type": "source_freshness",
                    "source": source_name,
                    "timestamp": timestamp,
                    "execution_time": execution_time,
                }
            )

        seen_sources = {w["source"] for w in warnings}
        for match in _SIMPLE_FRESHNESS_PATTERN.finditer(log_text):
            source_name = match.group(1)
            if source_name not in seen_sources:
                seen_sources.add(source_name)
                warnings.append(
                    {
                        "name": f"source_freshness_{source_name}",
                        "status": "WARN",
                        "type": "source_freshness",
                        "source": source_name,
                    }
                )

        return warnings


def setup_warning_handler(
    operator: Any,
    on_warning_callback: Callable[..., Any] | None,
    test_operator_class: type,
    source_operator_class: type,
) -> DbtTestWarningHandler | None:
    """Create a warning handler and attach it to the operator's callbacks list."""
    if not on_warning_callback:
        return None
    handler = DbtTestWarningHandler(
        on_warning_callback,
        operator=operator,
        test_operator_class=test_operator_class,
        source_operator_class=source_operator_class,
    )
    # Support for handling multiple operator callbacks via self.callbacks was added in provider version 10.2.0
    if isinstance(operator.callbacks, list):  # type: ignore[has-type]
        operator.callbacks.append(handler)  # type: ignore[has-type,arg-type]
    else:
        operator.callbacks = handler  # type: ignore[assignment]
    return handler


# This global variable is currently used to make the task context available to the K8s callback (set by the producer's execute()).
# While the callback is set during the operator initialization, the context is only created during the operator's execution.
# Safe because only one producer task runs per worker process at a time.
_producer_task_context: Any | None = None


class WatcherK8sCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """K8s pod log callback that parses dbt JSON output and pushes per-model XCom status."""

    @staticmethod
    def progress_callback(
        *,
        line: str,
        client: client_type,
        mode: str,
        container_name: str,
        timestamp: DateTime | None,
        pod: k8s.V1Pod,
        **kwargs: Any,
    ) -> None:
        """
        Invoke this callback to process pod container logs.

        Process a single pod log line through the dbt JSON parser.

        :param line: the read line of log.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        :param container_name: the name of the container from which the log line was read.
        :param timestamp: the timestamp of the log line.
        :param pod: the pod from which the log line was read.
        """
        if "context" not in kwargs:
            # This global variable is used to make the task context available to the K8s callback.
            # While the callback is set during the operator initialization, the context is only created during the operator's execution.
            kwargs["context"] = _producer_task_context
        store_dbt_resource_status_from_log(line, kwargs)


def init_watcher_producer(callback_class: type, kwargs: dict[str, Any]) -> None:
    """Normalise the ``callbacks`` kwarg and append the watcher callback class."""
    existing_callbacks = kwargs.get("callbacks")
    if existing_callbacks is None:
        normalized_callbacks: list[Any] = []
    elif isinstance(existing_callbacks, (list, tuple)):
        normalized_callbacks = list(existing_callbacks)
    else:
        normalized_callbacks = [existing_callbacks]
    normalized_callbacks.append(callback_class)
    kwargs["callbacks"] = normalized_callbacks


def execute_watcher_producer(operator: Any, context: Any, parent_execute: Callable[..., Any]) -> Any:
    """Shared ``execute`` logic for K8s watcher producer operators.

    Handles retry skipping and sets the module-level context global so that
    ``WatcherK8sCallback.progress_callback`` can access the task instance.
    """
    task_instance = context.get("ti")
    if task_instance is None:
        raise AirflowException(f"{type(operator).__name__} expects a task instance in the execution context")

    try_number = getattr(task_instance, "try_number", 1)

    if try_number > 1:
        operator.log.info(
            "%s does not support Airflow retries. "
            "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
            type(operator).__name__,
            try_number,
        )
        return None

    # This global variable is used to make the task context available to the K8s callback.
    # While the callback is set during the operator initialization, the context is only created
    # during the operator's execution.
    global _producer_task_context
    _producer_task_context = context
    return parent_execute(context)
