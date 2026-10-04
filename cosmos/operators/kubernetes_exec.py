"""Run dbt commands in existing Kubernetes containers."""

from __future__ import annotations

import inspect
import os
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
except ImportError as exc:
    raise ImportError(
        "Kubernetes exec mode requires apache-airflow-providers-cncf-kubernetes>=10.22.0. "
        "Install astronomer-cosmos[kubernetes-exec]."
    ) from exc

from cosmos.config import ProfileConfig
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

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]


class DbtKubernetesExecBaseOperator(AbstractDbtBase, KubernetesPodExecOperator):  # type: ignore[misc]
    """
    Execute dbt in an existing running container without managing the Pod lifecycle.

    The container must have dbt, its adapter, the project, dependencies and database
    credentials available. ``env`` overrides the container environment for this command
    only; it does not change the Pod specification. The container must provide ``env``.

    :param profile_config: Optional profile and target names. Profiles are not generated
        or copied into the container. Defaults to None.
    :param dbt_executable_path: dbt executable inside the container. Defaults to ``dbt``.
    :param on_warning_callback: Not supported in this execution mode. Defaults to None.
    """

    template_fields: Sequence[str] = tuple(
        dict.fromkeys((*AbstractDbtBase.template_fields, *KubernetesPodExecOperator.template_fields))
    )

    def __init__(
        self,
        profile_config: ProfileConfig | None = None,
        dbt_executable_path: str = "dbt",
        on_warning_callback: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if "command" in kwargs:
            raise ValueError("Cosmos builds the dbt command; use dbt_cmd_flags instead of command.")
        if on_warning_callback is not None:
            raise ValueError("on_warning_callback is not supported in Kubernetes exec mode.")
        # The converter passes the scheduler-side manifest to every execution mode.
        kwargs.pop("manifest_filepath", None)

        # AbstractDbtBase does not initialize BaseOperator; split its arguments before
        # initializing the provider operator, as in the other container execution modes.
        dbt_parameters = inspect.signature(AbstractDbtBase.__init__).parameters
        defaults = kwargs.get("default_args", {})
        dbt_kwargs = {
            name: kwargs.pop(name) if name in kwargs else defaults[name]
            for name, parameter in dbt_parameters.items()
            if parameter.kind not in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
            and name not in ("self", "dbt_executable_path")
            and (name in kwargs or name in defaults)
        }
        AbstractDbtBase.__init__(self, dbt_executable_path=dbt_executable_path, **dbt_kwargs)
        KubernetesPodExecOperator.__init__(self, command=[], **kwargs)
        self.profile_config = profile_config

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.invoke_interceptors(context)
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        if self.profile_config:
            dbt_cmd.extend(["--profile", self.profile_config.profile_name, "--target", self.profile_config.target_name])
        dbt_cmd.extend(["--project-dir", str(self.project_dir)])
        # Exec accepts argv, not a Pod env specification. No shell is involved, so
        # values containing spaces or shell metacharacters remain literal arguments.
        self.command = ["env", "--", *(f"{key}={os.fsdecode(value)}" for key, value in env.items()), *dbt_cmd]
        return KubernetesPodExecOperator.execute(self, context)


class DbtBuildKubernetesExecOperator(DbtBuildMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt build in an existing Kubernetes container."""

    template_fields: Sequence[str] = (
        *DbtKubernetesExecBaseOperator.template_fields,
        *DbtBuildMixin.template_fields,
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class DbtRunKubernetesExecOperator(DbtRunMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt run in an existing Kubernetes container."""

    template_fields: Sequence[str] = (*DbtKubernetesExecBaseOperator.template_fields, *DbtRunMixin.template_fields)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class DbtSeedKubernetesExecOperator(DbtSeedMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt seed in an existing Kubernetes container."""

    template_fields: Sequence[str] = (*DbtKubernetesExecBaseOperator.template_fields, *DbtSeedMixin.template_fields)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class DbtSnapshotKubernetesExecOperator(DbtSnapshotMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt snapshot in an existing Kubernetes container."""


class DbtTestKubernetesExecOperator(DbtTestMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt test in an existing Kubernetes container."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class DbtSourceKubernetesExecOperator(DbtSourceMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt source freshness in an existing Kubernetes container."""


class DbtLSKubernetesExecOperator(DbtLSMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt ls in an existing Kubernetes container."""


class DbtRunOperationKubernetesExecOperator(DbtRunOperationMixin, DbtKubernetesExecBaseOperator):
    """Execute a dbt macro in an existing Kubernetes container."""

    template_fields: Sequence[str] = (
        *DbtKubernetesExecBaseOperator.template_fields,
        *DbtRunOperationMixin.template_fields,
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class DbtCloneKubernetesExecOperator(DbtCloneMixin, DbtKubernetesExecBaseOperator):
    """Execute dbt clone in an existing Kubernetes container."""

    template_fields: Sequence[str] = (*DbtKubernetesExecBaseOperator.template_fields, "full_refresh")

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
