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
from typing import TYPE_CHECKING, Any, Protocol

import kubernetes.client as k8s
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.callbacks import client_type
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

try:
    from airflow.sdk.definitions.context import context_merge  # type: ignore[attr-defined]
except ImportError:
    from airflow.utils.context import context_merge

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.config import ProfileConfig
from cosmos.constants import PRODUCER_WATCHER_TASK_ID
from cosmos.operators._watcher.xcom import (
    _compose_backup_callback,
    _delete_xcom_backup_variable,
    _init_xcom_backup,
    _restore_xcom_from_variable,
)

if TYPE_CHECKING:  # pragma: no cover
    from pendulum import DateTime

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]
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


# The 2 classes below are an attempt at type hinting the operators. Not perfect but workable.
class K8sOperatorProtocol(Protocol):
    profile_config: ProfileConfig
    env_vars: list[k8s.V1EnvVar]
    cmds: list[str]
    arguments: list[str]
    project_dir: str
    log: Any
    callbacks: Any

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None: ...


class DbtK8sOperator(AbstractDbtBase, K8sOperatorProtocol): ...


def init_k8s_operator(
    operator: DbtK8sOperator,
    pod_operator_class: type[KubernetesPodOperator],
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
        operator_args.update(inspect.signature(clazz.__init__).parameters.keys())  # type: ignore[misc]
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


def _build_env_vars(env: dict[str, str | bytes | PathLike[Any]], existing_env_vars: list[Any]) -> list[k8s.V1EnvVar]:
    """Merge an env dict with existing K8s env vars and return the combined list."""
    env_vars_dict = {k: str(v) for k, v in env.items()}
    for ev in existing_env_vars:
        env_vars_dict[ev.name] = ev.value
    return convert_env_vars(env_vars_dict)  # type: ignore[no-any-return]


def build_kube_args(operator: DbtK8sOperator, context: Context, cmd_flags: list[str] | None = None) -> None:
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
    operator: DbtK8sOperator,
    pod_operator_class: type[KubernetesPodOperator],
    context: Context,
    cmd_flags: list[str] | None = None,
) -> Any:
    """Build kube args, log the command, and invoke the pod operator's ``execute``."""
    operator.invoke_interceptors(context)
    operator.build_kube_args(context, cmd_flags)
    # Log the full command (executable + arguments) for accurate, debuggable output.
    operator.log.info("Running command: %s", operator.cmds + operator.arguments)
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


class DbtTestWarningHandler(KubernetesPodOperatorCallback):  # type: ignore[misc]
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
        operator: DbtK8sOperator,
        test_operator_class: type,
        source_operator_class: type,
        context: Context | None = None,
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
            self.operator.log.warning("Cannot handle dbt warnings for task of type %s.", type(task))
            return

        # Get the logs from the pod
        logs = []
        for log in task.pod_manager.read_pod_logs(pod, "base"):  # type: ignore[attr-defined]
            decoded_log = log.decode("utf-8")
            if decoded_log != "":
                logs.append(decoded_log)

        logs_text = "\n".join(logs)

        # Check for warnings
        warning_detected = False
        if isinstance(task, self.test_operator_class):
            warn_count = self._detect_standard_warnings(logs_text)
            if warn_count:
                self.operator.log.info("Detected %s warnings using standard pattern", warn_count)
                warning_detected = True
        elif isinstance(task, self.source_operator_class):
            source_freshness_warnings = self._detect_source_freshness_warnings(logs_text)
            if source_freshness_warnings:
                self.operator.log.info("Detected %s source freshness warnings", len(source_freshness_warnings))
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
    operator: DbtK8sOperator,
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
    if isinstance(operator.callbacks, list):
        operator.callbacks.append(handler)
    else:
        operator.callbacks = handler
    return handler


# Contract keys between operator and callback.
CONTEXT_HOLDER_KEY = "context_holder"
CONTEXT_KEY = "context"


# The K8s-based watcher producers (``DbtProducerWatcherKubernetesOperator``,
# ``DbtProducerWatcherGcpGkeOperator``) share their per-execution state and the helpers below that
# read/write it. ``init_watcher_producer`` seeds that state; the operator carries the following
# contract (typed ``Any`` in the helpers, mirroring ``compose_watcher_backup_callbacks``, because
# the state is populated dynamically rather than declared on the concrete operator classes):
#   _tests_per_model, _test_results_per_model, _context_holder, _upstream_failure_skipped_ids,
#   _should_generate_model_uris, _dataset_namespace, _model_outlet_uris, manifest_filepath,
#   dbt_cmd_flags, profile_config, client, callbacks, log.


class WatcherK8sCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """K8s pod log callback that parses dbt JSON output and pushes per-model XCom status.

    ``tests_per_model``, ``test_results_per_model``, ``context_holder``, and
    ``upstream_failure_skipped_ids`` are forwarded by ``CosmosKubernetesPodManager``
    via its ``callback_extra_kwargs`` and arrive in ``progress_callback`` as ``**kwargs``.
    The ``receives_cosmos_callback_kwargs`` marker opts this callback in to receiving
    them; user-supplied callbacks without the marker are not given these Cosmos-only
    kwargs, so their ``progress_callback`` is not broken.

    ``context_holder`` is a mutable dict owned by the producer operator and captured
    by reference when the pod manager is created; ``execute()`` sets its ``"context"``
    entry, so this callback sees the live execution context no matter when the manager
    was created. ``progress_callback`` unwraps it into the plain ``context`` expected
    by ``store_dbt_resource_status_from_log``.
    """

    # Opts this callback in to receiving callback_extra_kwargs; read by
    # CosmosKubernetesPodManager._extra_kwargs_for (keep the attribute name in sync).
    receives_cosmos_callback_kwargs = True

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
        # Don't leak other callback kwargs: the parser only needs "context" in this case.
        holder = kwargs.get(CONTEXT_HOLDER_KEY)
        context = holder.get(CONTEXT_KEY) if holder else None
        # `store_dbt_resource_status_from_log` expects context be passed in a dict of this shape.
        extra_kwargs = {"context": context} if context else {}
        store_dbt_resource_status_from_log(
            line,
            extra_kwargs,
            tests_per_model=kwargs.get("tests_per_model"),
            test_results_per_model=kwargs.get("test_results_per_model"),
            # The producer pre-populates model_outlet_uris from the scheduler-side manifest
            # (the pod's manifest isn't reachable from here), so the parser's lazy fill --
            # which needs project_dir -- is a no-op and dataset_namespace is left unset.
            model_outlet_uris=kwargs.get("model_outlet_uris"),
            should_generate_model_uris=kwargs.get("should_generate_model_uris", True),
            upstream_failure_skipped_ids=kwargs.get("upstream_failure_skipped_ids"),
        )


def inject_watcher_callback(kwargs: dict[str, Any]) -> None:
    """Normalize the ``callbacks`` kwarg and append ``WatcherK8sCallback``."""
    existing_callbacks = kwargs.get("callbacks")
    if existing_callbacks is None:
        normalized_callbacks: list[Any] = []
    elif isinstance(existing_callbacks, (list, tuple)):
        normalized_callbacks = list(existing_callbacks)
    else:
        normalized_callbacks = [existing_callbacks]
    normalized_callbacks.append(WatcherK8sCallback)
    kwargs["callbacks"] = normalized_callbacks


def init_watcher_producer(operator: Any, kwargs: dict[str, Any]) -> str:
    """Shared pre-``super().__init__`` setup for K8s-based watcher producers.

    Seeds the per-execution state that ``build_watcher_pod_manager`` / ``execute_watcher_producer``
    rely on, injects the log-parsing callback, and returns the resolved ``task_id`` for the caller
    to forward to ``super().__init__``. Shared by ``DbtProducerWatcherKubernetesOperator`` and
    ``DbtProducerWatcherGcpGkeOperator``.

    :param operator: the producer instance being initialised.
    :param kwargs: the operator's ``__init__`` kwargs; watcher-only keys are popped in place.
    :returns: the resolved ``task_id``.
    """
    task_id: str = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
    operator._tests_per_model = kwargs.pop("tests_per_model", {})
    operator._test_results_per_model = {}
    # Whether the producer should compute per-model outlet URIs. The producer never emits datasets
    # itself (the consumer sensors do), but it must build the URI map so each consumer can. Wired as
    # an explicit flag by _add_watcher_producer_task.
    operator._should_generate_model_uris = kwargs.pop("_should_generate_model_uris", kwargs.get("emit_datasets", True))
    # manifest_filepath is threaded through task_args by cosmos.converter (from
    # ProjectConfig.manifest_path). The pod's own target/manifest.json lives inside the container and
    # is not reachable from the scheduler, so this scheduler-side manifest is the only practical
    # source for the outlet URI map. Popped here because the K8s base operator (unlike the local one)
    # doesn't accept it.
    operator.manifest_filepath = kwargs.pop("manifest_filepath", "") or ""
    # Mutable per-execution state shared by reference with the log-parsing callback via the pod
    # manager's callback_extra_kwargs. execute() resolves the namespace and fills the URI map in
    # place (never reassigns), so a pod_manager created earlier still observes the populated map.
    operator._dataset_namespace = None
    operator._model_outlet_uris = {}
    # Mutable holder shared by reference with pod_manager's callback_extra_kwargs. execute() sets its
    # "context" entry (the holder itself is never reassigned), so a pod_manager created before
    # execute() still sees the live context.
    operator._context_holder = {CONTEXT_KEY: None}
    inject_watcher_callback(kwargs)
    return task_id


def finalize_watcher_producer(operator: Any) -> None:
    """Shared post-``super().__init__`` setup for K8s-based watcher producers.

    Forces the dbt JSON log format the parser depends on, wires the XCom-backup flush onto the
    producer's retry/failure callbacks, and seeds the upstream-failure tracking set. Must run after
    ``super().__init__`` so ``compose_watcher_backup_callbacks`` can preserve a DAG-level
    ``default_args`` callback (#2776).
    """
    operator.dbt_cmd_flags += ["--log-format", "json"]
    compose_watcher_backup_callbacks(operator)
    # Populated by the log parser when dbt emits SkippingDetails or LogSkipBecauseError for a node;
    # subsequent "skipped" terminal events for those unique_ids are rewritten to "failed" so the
    # consumer sensor fails on attempt 1 (instead of SKIPPED, which Airflow will not retry).
    # Mirrors DbtProducerWatcherOperator._upstream_failure_skipped_ids; see #2698.
    operator._upstream_failure_skipped_ids = set()


def compose_watcher_backup_callbacks(operator: Any) -> None:
    """Append the XCom backup flush to the producer's retry/failure callbacks.

    A graceful failure with retries left is UP_FOR_RETRY (on_retry_callback), not
    FAILED, so register on both. Must be called after ``super().__init__`` to preserve
    a DAG-level ``default_args`` callback (#2776).
    """
    operator.on_retry_callback = _compose_backup_callback(getattr(operator, "on_retry_callback", None))
    operator.on_failure_callback = _compose_backup_callback(getattr(operator, "on_failure_callback", None))


def build_watcher_pod_manager(operator: Any) -> CosmosKubernetesPodManager:
    """Build the producer's pod manager, threading watcher state to ``WatcherK8sCallback``.

    The pod manager forwards ``callback_extra_kwargs`` only to callbacks marked with
    ``receives_cosmos_callback_kwargs`` (i.e. ``WatcherK8sCallback``).
    """
    return CosmosKubernetesPodManager(
        kube_client=operator.client,
        callbacks=operator.callbacks,
        callback_extra_kwargs={
            "tests_per_model": operator._tests_per_model,
            "test_results_per_model": operator._test_results_per_model,
            CONTEXT_HOLDER_KEY: operator._context_holder,
            "upstream_failure_skipped_ids": operator._upstream_failure_skipped_ids,
            # _model_outlet_uris is a dict shared by reference; execute_watcher_producer fills
            # it in place before the pod runs, so the callback reads the populated map even
            # though the pod manager (a cached_property) may be built earlier.
            "model_outlet_uris": operator._model_outlet_uris,
            "should_generate_model_uris": operator._should_generate_model_uris,
        },
    )


def _populate_producer_model_outlet_uris(operator: Any) -> None:
    """Resolve the dataset namespace and fill ``operator._model_outlet_uris`` from the manifest.

    Mirrors the SUBPROCESS producer, but reads ``ProjectConfig.manifest_path`` (threaded as
    ``manifest_filepath``) instead of ``{project_dir}/target/manifest.json``: in K8s the pod's
    manifest isn't reachable from the scheduler. The map is mutated in place (never reassigned)
    so the reference held by the pod manager's ``callback_extra_kwargs`` stays valid.

    Degrades to a no-op -- dbt still runs and statuses are still reported, but no datasets are
    emitted -- when generation is disabled, no ``ProfileConfig`` is set, no namespace resolves,
    or the manifest is unavailable.
    """
    operator._model_outlet_uris.clear()
    operator._dataset_namespace = None
    if not operator._should_generate_model_uris:
        return
    # get_dataset_namespace requires a ProfileConfig; some constructions (e.g. inline profiles
    # via profiles_yml_filepath only) don't supply one, so dataset emission degrades to a no-op.
    if operator.profile_config is None:
        return

    from cosmos.dataset import compute_model_outlet_uris, get_dataset_namespace

    operator._dataset_namespace = get_dataset_namespace(operator.profile_config)
    if not operator._dataset_namespace:
        return
    if not operator.manifest_filepath:
        operator.log.warning(
            "manifest_filepath not supplied to %s; per-model dataset emission is disabled for this run. "
            "Pass ProjectConfig.manifest_path to enable it.",
            type(operator).__name__,
        )
        return
    # manifest_filepath is ProjectConfig.manifest_path, an Airflow ObjectStoragePath that may point
    # at a remote manifest (s3://, gs://, ...). Pass it through unchanged -- wrapping it in Path()
    # would mangle remote URIs (e.g. "s3://b/m.json" -> "s3:/b/m.json"). compute_model_outlet_uris
    # reads it via ObjectStoragePath.open() and returns {} (logging) if it's missing or unreadable,
    # so no local existence check is needed here.
    operator._model_outlet_uris.update(
        compute_model_outlet_uris(operator.manifest_filepath, operator._dataset_namespace)
    )


def execute_watcher_producer(
    operator: Any, context: Context, parent_execute: Callable[..., Any], **kwargs: Any
) -> Any:
    """Shared ``execute`` logic for K8s watcher producer operators.

    On retry, restores any XCom backup and raises ``AirflowSkipException`` (the
    producer does not support Airflow retries). On the first attempt, initialises
    an XCom backup, exposes the execution context to the log-parsing callback, runs
    the parent execute, and deletes the backup on success. On failure the producer's
    on-failure/on-retry callback (see ``compose_watcher_backup_callbacks``) flushes
    the backup so the next try can restore it.
    """
    task_instance = context.get("ti")
    if task_instance is None:
        raise AirflowException(f"{type(operator).__name__} expects a task instance in the execution context")

    try_number = getattr(task_instance, "try_number", 1)

    from cosmos import settings

    reliable_retry = settings.enable_watcher_reliable_retry

    if try_number > 1:
        _restore_xcom_from_variable(context)
        raise AirflowSkipException(
            f"{type(operator).__name__} does not support Airflow retries. "
            f"Detected attempt #{try_number}; skipping execution to avoid running a second dbt build."
        )

    _init_xcom_backup(context, persist=reliable_retry)

    # Resolve the namespace and build the per-model outlet URI map before the pod runs, so the
    # log-parsing callback can attach outlet URIs to each model's status XCom for the consumers.
    _populate_producer_model_outlet_uris(operator)
    operator._upstream_failure_skipped_ids.clear()
    # Publish the context through the mutable holder shared by reference with the pod
    # manager's callback_extra_kwargs, so the log-parsing callback sees the live context
    # even if the pod manager (a cached_property) was created before this runs.
    operator._context_holder[CONTEXT_KEY] = context

    # On failure parent_execute() raises and the on-failure callback flushes the backup.
    return_value = parent_execute(context, **kwargs)
    if reliable_retry:
        _delete_xcom_backup_variable(context)
    return return_value
