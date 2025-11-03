from __future__ import annotations

import inspect
import re
from abc import ABC
from os import PathLike
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence

import kubernetes.client as k8s
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import context_merge

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    # apache-airflow-providers-cncf-kubernetes >= 7.14.0
    from airflow.providers.cncf.kubernetes.callbacks import (
        KubernetesPodOperatorCallback,
    )
except ImportError:

    class KubernetesPodOperatorCallback:  # type: ignore[no-redef]
        """Mock fallback for older versions. Should not be used in practice."""

        pass


from cosmos.config import ProfileConfig
from cosmos.dbt.parser.output import extract_log_issues
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2


class DbtKubernetesBaseOperator(AbstractDbtBase, KubernetesPodOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(KubernetesPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        self.profile_config = profile_config

        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}
        operator_args: set[str] = set()
        for clazz in KubernetesPodOperator.__mro__:
            operator_args.update(inspect.signature(clazz.__init__).parameters.keys())
            if clazz == BaseOperator:
                break
        for arg in operator_args:
            try:
                operator_kwargs[arg] = kwargs[arg]
            except KeyError:
                pass

        base_kwargs = {}
        for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
            try:
                base_kwargs[arg] = kwargs[arg]
            except KeyError:
                try:
                    base_kwargs[arg] = default_args[arg]
                except KeyError:
                    pass

        AbstractDbtBase.__init__(self, **base_kwargs)

        container_resources = operator_kwargs.get("container_resources")
        if isinstance(container_resources, dict):
            operator_kwargs["container_resources"] = k8s.V1ResourceRequirements(**container_resources)
        KubernetesPodOperator.__init__(self, **operator_kwargs)

    def build_env_args(self, env: dict[str, str | bytes | PathLike[Any]]) -> None:
        env_vars_dict: dict[str, str] = dict()

        for env_var_key, env_var_value in env.items():
            env_vars_dict[env_var_key] = str(env_var_value)
        for env_var in self.env_vars:
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars: list[Any] = convert_env_vars(env_vars_dict)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.build_kube_args(context, cmd_flags)
        self.log.info(f"Running command: {self.arguments}")
        result = KubernetesPodOperator.execute(self, context)
        self.log.info(result)

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)

        # Parse ProfileConfig and add additional arguments to the dbt_cmd
        if self.profile_config:
            if self.profile_config.profile_name:
                dbt_cmd.extend(["--profile", self.profile_config.profile_name])
            if self.profile_config.target_name:
                dbt_cmd.extend(["--target", self.profile_config.target_name])

        if self.project_dir:
            dbt_cmd.extend(["--project-dir", str(self.project_dir)])

        # set env vars
        self.build_env_args(env_vars)
        self.arguments = dbt_cmd


class DbtBuildKubernetesOperator(DbtBuildMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSKubernetesOperator(DbtLSMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedKubernetesOperator(DbtSeedMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotKubernetesOperator(DbtSnapshotMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWarningHandler(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """
    This handler can detect warnings from:
    1. Regular dbt tests (using the standard "Done. PASS=X WARN=Y" pattern)
    2. Source freshness tests (using "WARN freshness of..." pattern)
    """

    def __init__(
        self,
        on_warning_callback: Callable[..., Any],
        operator: KubernetesPodOperator,
        context: Optional[Context] = None,
    ) -> None:
        self.on_warning_callback = on_warning_callback
        self.operator = operator
        self.context = context

    def on_pod_completion(  # type: ignore[override]
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
        if not (isinstance(task, DbtTestKubernetesOperator) or isinstance(task, DbtSourceKubernetesOperator)):
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
        if isinstance(task, DbtTestKubernetesOperator):
            warn_count = self._detect_standard_warnings(logs_text)
            if warn_count:
                self.operator.log.info(f"Detected {warn_count} warnings using standard pattern")
                warning_detected = True
        elif isinstance(task, DbtSourceKubernetesOperator):
            source_freshness_warnings = self._detect_source_freshness_warnings(logs_text)
            if source_freshness_warnings:
                self.operator.log.info(f"Detected {len(source_freshness_warnings)} source freshness warnings")
                warning_detected = True

        if not warning_detected:
            self.operator.log.warning(
                "Failed to scrape warning count from the pod logs."
                "Potential warning callbacks could not be triggered."
            )
            return

        test_names, test_results = extract_log_issues(logs)
        context_merge(self.context, test_names=test_names, test_results=test_results)
        self.on_warning_callback(self.context)

    def _detect_standard_warnings(self, log_text: str) -> Optional[int]:
        """
        Detect warnings using the standard dbt summary pattern.

        Pattern: "Done. PASS=X WARN=Y ERROR=Z SKIP=W"

        :param log_text: Complete log text from the pod
        :return: Number of warnings detected, or None if pattern not found
        """
        warn_count_pattern = re.compile(r"Done\. (?:\w+=\d+ )*WARN=(\d+)(?: \w+=\d+)*")
        match = warn_count_pattern.search(log_text)

        if match:
            return int(match.group(1))
        return None

    def _detect_source_freshness_warnings(self, log_text: str) -> List[Dict[str, Any]]:
        """
        Detect source freshness warnings from dbt logs.

        Pattern examples:
        - "15:49:21 1 of 1 WARN freshness of auction_net.auction_net_raw ... [WARN in 0.90s]"
        - "WARN freshness of source_name.table_name"

        :param log_text: Complete log text from the pod
        :return: List of warning dictionaries
        """
        warnings = []

        # Primary pattern for source freshness warnings
        # Matches: "HH:MM:SS X of Y WARN freshness of source.table ... [WARN in Xs]"
        freshness_pattern = re.compile(
            r"(\d{2}:\d{2}:\d{2})\s+"  # timestamp
            r"\d+\s+of\s+\d+\s+"  # "X of Y"
            r"WARN\s+freshness\s+of\s+"  # "WARN freshness of"
            r"([^\s]+)"  # source name
            r".*?\[WARN\s+in\s+([\d.]+)s\]"  # execution time
        )

        for match in freshness_pattern.finditer(log_text):
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

        # Secondary pattern for simpler source freshness warnings
        # Matches: "WARN freshness of source_name"
        simple_freshness_pattern = re.compile(r"WARN\s+freshness\s+of\s+([^\s]+)")

        for match in simple_freshness_pattern.finditer(log_text):
            source_name = match.group(1)
            # Only add if not already captured by primary pattern
            if not any(w["source"] == source_name for w in warnings):
                warnings.append(
                    {
                        "name": f"source_freshness_{source_name}",
                        "status": "WARN",
                        "type": "source_freshness",
                        "source": source_name,
                    }
                )

        return warnings


class DbtWarningKubernetesOperator(DbtKubernetesBaseOperator, ABC):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.warning_handler = None
        if on_warning_callback:
            self.warning_handler = DbtTestWarningHandler(on_warning_callback, operator=self)
            # Support for handling multiple operator callbacks via self.callbacks was added in provider version 10.2.0
            if isinstance(self.callbacks, list):  # type: ignore[has-type]
                self.callbacks.append(self.warning_handler)  # type: ignore[has-type,arg-type]
            else:
                self.callbacks = self.warning_handler  # type: ignore[assignment]

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        if self.warning_handler:
            self.warning_handler.context = context
        super().build_and_run_cmd(
            context=context, cmd_flags=cmd_flags, run_as_async=run_as_async, async_context=async_context
        )


class DbtTestKubernetesOperator(DbtTestMixin, DbtWarningKubernetesOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceKubernetesOperator(DbtSourceMixin, DbtWarningKubernetesOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunKubernetesOperator(DbtRunMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunOperationKubernetesOperator(DbtRunOperationMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run-operation command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneKubernetesOperator(DbtCloneMixin, DbtKubernetesBaseOperator):
    """Executes a dbt core clone command."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
