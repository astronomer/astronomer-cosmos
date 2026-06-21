from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

import cosmos.operators._k8s_common as _k8s_common
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

try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:  # Since Airflow 3.1, BaseHook is in the airflow.sdk.bases.hook module
    from airflow.hooks.base import BaseHook


class DbtKubernetesBaseOperator(AbstractDbtBase, KubernetesPodOperator):  # type: ignore[misc]
    """
    Executes a dbt core cli command in a Kubernetes Pod.
    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(KubernetesPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        _k8s_common.init_k8s_operator(self, KubernetesPodOperator, profile_config, kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        _k8s_common.build_and_run_cmd(self, KubernetesPodOperator, context, cmd_flags)

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        _k8s_common.build_kube_args(self, context, cmd_flags)


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


class DbtWarningKubernetesOperator(DbtKubernetesBaseOperator, ABC):
    """
    Base for dbt operators that detect and handle test/source freshness warnings.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.warning_handler = _k8s_common.setup_warning_handler(
            self, on_warning_callback, DbtTestKubernetesOperator, DbtSourceKubernetesOperator
        )

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


class DbtDocsKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes `dbt docs generate` command.
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields

    ui_color = "#8194E0"
    required_files = ["index.html", "manifest.json", "catalog.json"]
    base_cmd = ["docs", "generate"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.check_static_flag()

    def check_static_flag(self) -> None:
        if self.dbt_cmd_flags:
            if "--static" in self.dbt_cmd_flags:
                # For the --static flag we only upload the generated static_index.html file
                self.required_files = ["static_index.html"]
        if self.dbt_cmd_global_flags:
            if "--no-write-json" in self.dbt_cmd_global_flags and "graph.gpickle" in self.required_files:
                self.required_files.remove("graph.gpickle")


class DbtDocsCloudKubernetesOperator(DbtDocsKubernetesOperator, ABC):
    """
    Executes `dbt docs generate` inside a Kubernetes Pod and uploads
    the generated documentation to cloud storage *also inside the Pod*.
    """

    template_fields: Sequence[str] = DbtDocsKubernetesOperator.template_fields

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # In Kubernetes mode, we do NOT use callback-based upload on the Airflow worker.
        self.callback = None

    @abstractmethod
    def build_upload_shell_command(self, docs_target: str) -> str:
        """
        Build the shell command that will upload the generated docs from
        `docs_target` to cloud storage. Implemented by subclasses.
        """

    @abstractmethod
    def get_upload_env_vars(self) -> dict[str, str]:
        """Return env vars required by the upload command."""

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.inject_upload_env_vars(self.get_upload_env_vars())

        # Build base Kubernetes pod args (incl. dbt CLI command)
        self.build_kube_args(context, cmd_flags)

        # build_kube_args splits the executable into self.cmds (e.g. ["dbt"]) and the
        # remaining tokens into self.arguments; recombine both so the leading "dbt" is not
        # dropped when the command is folded into the bash -c string below (see PR #2488).
        cmds: Any = self.cmds  # type: ignore[has-type]
        arguments: Any = self.arguments  # type: ignore[has-type]
        cmd_parts = list(cmds) if isinstance(cmds, (list, tuple)) else [cmds]
        if isinstance(arguments, (list, tuple)):
            cmd_parts.extend(arguments)
        else:
            cmd_parts.append(arguments)

        dbt_cmd_str = " ".join([str(part) for part in cmd_parts])
        docs_target = f"{self.project_dir}/target"

        upload_cmd = self.build_upload_shell_command(docs_target)
        shell_cmd = f"{dbt_cmd_str} && {upload_cmd}"

        # Override container command and arguments
        self.cmds = ["/bin/bash", "-c"]
        self.arguments = [shell_cmd]

        self.log.info("Running command in Kubernetes Pod: %s", self.arguments)
        result = KubernetesPodOperator.execute(self, context)
        self.log.info(result)
        return result

    def inject_upload_env_vars(self, env_vars: dict[str, str]) -> None:
        existing_env_vars: list[Any] = list(self.env_vars or [])  # type: ignore[has-type]
        declared_env_var_names = {env_var.name for env_var in existing_env_vars if getattr(env_var, "name", None)} | {
            secret.deploy_target
            for secret in self.secrets or []
            if getattr(secret, "deploy_type", None) == "env" and getattr(secret, "deploy_target", None)
        }

        missing_env_vars = {
            key: value for key, value in env_vars.items() if value and key not in declared_env_var_names
        }

        if not missing_env_vars:
            return

        self.env_vars = existing_env_vars + convert_env_vars(missing_env_vars)


class DbtDocsS3KubernetesOperator(DbtDocsCloudKubernetesOperator):
    """
    Executes `dbt docs generate` inside a Kubernetes Pod and uploads the generated
    documentation to S3 *also inside that Pod* using `aws s3 sync`.
        - The Kubernetes Pod receives AWS credentials resolved from the supplied
          Airflow `connection_id`.
    """

    ui_color = "#FF9900"

    def __init__(
        self,
        connection_id: str,
        bucket_name: str,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir

    def build_upload_shell_command(self, docs_target: str) -> str:
        if self.folder_dir:
            s3_prefix = f"s3://{self.bucket_name}/{self.folder_dir}".rstrip("/")
        else:
            s3_prefix = f"s3://{self.bucket_name}"

        return f"aws s3 sync {docs_target} {s3_prefix}"

    def get_upload_env_vars(self) -> dict[str, str]:
        return self.aws_env_vars_from_connection(self.connection_id)

    def aws_env_vars_from_connection(self, connection_id: str) -> dict[str, str]:
        conn = BaseHook.get_connection(connection_id)
        conn_extra = conn.extra_dejson

        access_key = conn.login or conn_extra.get("aws_access_key_id")
        secret_key = conn.password or conn_extra.get("aws_secret_access_key")
        session_token = conn_extra.get("aws_session_token") or conn_extra.get("session_token")

        session_kwargs = conn_extra.get("session_kwargs", {})
        config_kwargs = conn_extra.get("config_kwargs", {})
        if not isinstance(session_kwargs, dict):
            session_kwargs = {}
        if not isinstance(config_kwargs, dict):
            config_kwargs = {}
        region_name = (
            conn_extra.get("region_name")
            or conn_extra.get("region")
            or session_kwargs.get("region_name")
            or config_kwargs.get("region_name")
        )

        env_vars = {}
        if access_key:
            env_vars["AWS_ACCESS_KEY_ID"] = access_key
        if secret_key:
            env_vars["AWS_SECRET_ACCESS_KEY"] = secret_key
        if session_token:
            env_vars["AWS_SESSION_TOKEN"] = session_token
        if region_name:
            env_vars["AWS_DEFAULT_REGION"] = region_name

        return env_vars
