from __future__ import annotations

import inspect
import re
from abc import ABC
from collections.abc import Callable, Sequence
from os import PathLike
from typing import TYPE_CHECKING, Any

try:  # Optional dependency (python kubernetes client)
    import kubernetes.client as k8s  # type: ignore
except ImportError:  # pragma: no cover
    k8s = None  # type: ignore[assignment]

from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
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


def _require_kubernetes_client() -> Any:
    """
    Return the kubernetes client module, or raise a helpful ImportError if the optional dependency is missing.
    """
    if k8s is None:
        raise ImportError(
            "Missing optional dependency 'kubernetes' (the Python Kubernetes client). "
            "Install it to use ExecutionMode.GKE (e.g. `pip install kubernetes`)."
        )
    return k8s


class DbtGkeBaseOperator(AbstractDbtBase, GKEStartPodOperator):  # type: ignore[misc]
    """
    Executes a dbt core cli command in a Google Kubernetes Engine (GKE) Pod using GKEStartPodOperator.

    This is a near-parallel of cosmos.operators.kubernetes, but swaps the underlying operator to
    GKEStartPodOperator to support:
      - remote/external clusters (project/location/cluster_name/namespace)
      - deferrable=True execution mode
    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(GKEStartPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        self.profile_config = profile_config

        # See kubernetes.py for rationale:
        # We explicitly initialize parent classes because AbstractDbtBase no longer calls super().__init__().
        default_args = kwargs.get("default_args", {})

        # Collect kwargs accepted by the underlying operator (and BaseOperator) using MRO.
        operator_kwargs: dict[str, Any] = {}
        operator_args: set[str] = set()
        for clazz in GKEStartPodOperator.__mro__:
            operator_args.update(inspect.signature(clazz.__init__).parameters.keys())
            if clazz == BaseOperator:
                break
        for arg in operator_args:
            if arg in kwargs:
                operator_kwargs[arg] = kwargs[arg]

        # Collect kwargs accepted by AbstractDbtBase (from kwargs first, then default_args)
        base_kwargs: dict[str, Any] = {}
        for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
            if arg in kwargs:
                base_kwargs[arg] = kwargs[arg]
            elif arg in default_args:
                base_kwargs[arg] = default_args[arg]

        AbstractDbtBase.__init__(self, **base_kwargs)

        # Normalize container_resources (dict -> V1ResourceRequirements), consistent with kubernetes.py.
        container_resources = operator_kwargs.get("container_resources")
        if isinstance(container_resources, dict):
            _k8s = _require_kubernetes_client()
            operator_kwargs["container_resources"] = _k8s.V1ResourceRequirements(**container_resources)

        # Initialize the underlying GKE operator
        GKEStartPodOperator.__init__(self, **operator_kwargs)

    def build_env_args(self, env: dict[str, str | bytes | PathLike[Any]]) -> None:
        env_vars_dict: dict[str, str] = {}

        for env_var_key, env_var_value in env.items():
            env_vars_dict[env_var_key] = str(env_var_value)

        # Preserve any env_vars already configured on the operator
        for env_var in getattr(self, "env_vars", []) or []:
            # env_var can be V1EnvVar or dict-like depending on provider/backcompat
            env_vars_dict[getattr(env_var, "name", None) or env_var["name"]] = (
                getattr(env_var, "value", None) or env_var["value"]
            )

        self.env_vars = convert_env_vars(env_vars_dict)  # type: ignore[assignment]

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
        result = GKEStartPodOperator.execute(self, context)
        self.log.info(result)
        return result

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For now we assume dbt is in PATH inside the image.
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


class DbtBuildGkeOperator(DbtBuildMixin, DbtGkeBaseOperator):
    """Executes a dbt core build command."""

    template_fields: Sequence[str] = DbtGkeBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSGkeOperator(DbtLSMixin, DbtGkeBaseOperator):
    """Executes a dbt core ls command."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedGkeOperator(DbtSeedMixin, DbtGkeBaseOperator):
    """Executes a dbt core seed command."""

    template_fields: Sequence[str] = DbtGkeBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotGkeOperator(DbtSnapshotMixin, DbtGkeBaseOperator):
    """Executes a dbt core snapshot command."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWarningHandler(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """
    Detects dbt warnings from pod logs and calls the provided callback with enriched context.

    Works for:
      1) Standard dbt tests (Done. PASS=X WARN=Y ...)
      2) Source freshness tests (WARN freshness of ...)
    """

    def __init__(
        self,
        on_warning_callback: Callable[..., Any],
        operator: Any,
        context: Context | None = None,
    ) -> None:
        self.on_warning_callback = on_warning_callback
        self.operator = operator
        self.context = context

    def on_pod_completion(  # type: ignore[override]
        self,
        *,
        pod: Any,
        **kwargs: Any,
    ) -> None:
        if not self.context:
            self.operator.log.warning("No context provided to the DbtTestWarningHandler.")
            return

        task = self.context["task_instance"].task
        if not (isinstance(task, DbtTestGkeOperator) or isinstance(task, DbtSourceGkeOperator)):
            self.operator.log.warning(f"Cannot handle dbt warnings for task of type {type(task)}.")
            return

        # Read logs from the pod (container name "base" matches Cosmos conventions)
        logs: list[bytes] = []
        for log in task.pod_manager.read_pod_logs(pod, "base"):
            if log:
                logs.append(log)

        decoded_logs: list[str] = [l.decode("utf-8") for l in logs if l]
        logs_text = "\n".join(decoded_logs).strip()

        warning_detected = False
        if isinstance(task, DbtTestGkeOperator):
            warn_count = self._detect_standard_warnings(logs_text)
            if warn_count:
                self.operator.log.info(f"Detected {warn_count} warnings using standard pattern")
                warning_detected = True
        elif isinstance(task, DbtSourceGkeOperator):
            source_freshness_warnings = self._detect_source_freshness_warnings(logs_text)
            if source_freshness_warnings:
                self.operator.log.info(f"Detected {len(source_freshness_warnings)} source freshness warnings")
                warning_detected = True

        if not warning_detected:
            self.operator.log.warning(
                "Failed to scrape warning count from the pod logs. Potential warning callbacks could not be triggered."
            )
            return

        test_names, test_results = extract_log_issues(decoded_logs)
        context_merge(self.context, test_names=test_names, test_results=test_results)
        self.on_warning_callback(self.context)

    def _detect_standard_warnings(self, log_text: str) -> int | None:
        warn_count_pattern = re.compile(r"Done\. (?:\w+=\d+ )*WARN=(\d+)(?: \w+=\d+)*")
        match = warn_count_pattern.search(log_text)
        if match:
            return int(match.group(1))
        return None

    def _detect_source_freshness_warnings(self, log_text: str) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []

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

        simple_freshness_pattern = re.compile(r"WARN\s+freshness\s+of\s+([^\s]+)")
        for match in simple_freshness_pattern.finditer(log_text):
            source_name = match.group(1)
            if not any(w.get("source") == source_name for w in warnings):
                warnings.append(
                    {
                        "name": f"source_freshness_{source_name}",
                        "status": "WARN",
                        "type": "source_freshness",
                        "source": source_name,
                    }
                )

        return warnings


class DbtWarningGkeOperator(DbtGkeBaseOperator, ABC):
    """Base for dbt commands where warning callbacks should be supported."""

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.warning_handler: DbtTestWarningHandler | None = None

        if not on_warning_callback:
            return

        self.warning_handler = DbtTestWarningHandler(on_warning_callback, operator=self)

        # Some provider/operator versions (notably older GKEStartPodOperator) don't expose `callbacks`.
        existing = getattr(self, "callbacks", None)

        # If callbacks aren't supported, we still keep the handler; tests can call it explicitly,
        # and runtime can wire via other mechanisms if needed.
        if existing is None:
            return

        # Provider >= 10.2.0 supports list callbacks; older versions may be a single callback object.
        if isinstance(existing, list):
            existing.append(self.warning_handler)
        else:
            setattr(self, "callbacks", self.warning_handler)

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
        return super().build_and_run_cmd(
            context=context, cmd_flags=cmd_flags, run_as_async=run_as_async, async_context=async_context
        )


class DbtTestGkeOperator(DbtTestMixin, DbtWarningGkeOperator):
    """Executes a dbt core test command."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceGkeOperator(DbtSourceMixin, DbtWarningGkeOperator):
    """Executes a dbt source freshness command."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunGkeOperator(DbtRunMixin, DbtGkeBaseOperator):
    """Executes a dbt core run command."""

    template_fields: Sequence[str] = DbtGkeBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunOperationGkeOperator(DbtRunOperationMixin, DbtGkeBaseOperator):
    """Executes a dbt core run-operation command."""

    template_fields: Sequence[str] = (
        DbtGkeBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneGkeOperator(DbtCloneMixin, DbtGkeBaseOperator):
    """Executes a dbt core clone command."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
