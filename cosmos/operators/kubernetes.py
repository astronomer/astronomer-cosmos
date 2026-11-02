from __future__ import annotations

import json
import shlex
import textwrap
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from pathlib import Path
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
from cosmos.exceptions import CosmosValueError
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSemanticMetricViewMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)


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


class DbtSemanticMetricViewKubernetesOperator(DbtSemanticMetricViewMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command against an adapter-native semantic metric view.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtSemanticMetricViewMixin.template_fields  # type: ignore[operator]

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
        Build the shell command that uploads generated docs from `docs_target`
        to cloud storage inside the Kubernetes Pod.
        """

    @abstractmethod
    def get_upload_env_vars(self) -> dict[str, str]:
        """Return env vars required by the upload command."""

    @staticmethod
    def _run_python_script_command(script: str) -> str:
        """
        Build the shell command that runs ``script`` with the Python of the image.

        The interpreter path is quoted so that a path with spaces is not split by the shell.
        """
        return f"\"$(command -v python3 || command -v python)\" - <<'PY'\n{script}\nPY"

    @staticmethod
    def _command_parts(command: Any) -> list[str]:
        if not command:
            return []
        if isinstance(command, (list, tuple)):
            return [str(part) for part in command]
        return [str(command)]

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

        # build_kube_args may place the executable in either self.cmds (when cmds is explicitly
        # ["dbt"]) or self.arguments (the default, where cmds is left unset); recombine both so
        # the leading "dbt" is not dropped when folded into the bash -c string below (see PR #2488).
        cmds: Any = self.cmds  # type: ignore[has-type]
        arguments: Any = self.arguments  # type: ignore[has-type]
        cmd_parts = self._command_parts(cmds) + self._command_parts(arguments)
        dbt_cmd_str = shlex.join(cmd_parts)
        docs_target = f"{self.project_dir}/target"

        upload_cmd = self.build_upload_shell_command(docs_target)
        shell_cmd = f"{dbt_cmd_str} && {upload_cmd}"

        self.cmds = ["/bin/bash", "-c"]
        self.arguments = [shell_cmd]

        self.log.info("Running command in Kubernetes Pod: %s", self.arguments)
        result = KubernetesPodOperator.execute(self, context)
        self.log.info(result)

        return result

    def inject_upload_env_vars(self, env_vars: dict[str, str]) -> None:
        current_env_vars = self.env_vars or []  # type: ignore[has-type]
        # cncf-kubernetes >= 10.23.0 leaves a user-supplied ``env_vars`` dict unconverted until pod build time
        if isinstance(current_env_vars, dict):
            current_env_vars = convert_env_vars(current_env_vars)
        existing_env_vars: list[Any] = list(current_env_vars)
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
    documentation to S3 also inside that Pod using ``boto3``.
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
        folder_dir = self.folder_dir.rstrip("/") if self.folder_dir else ""
        upload_script = textwrap.dedent(f"""
            import json
            import mimetypes
            import os
            from pathlib import Path

            try:
                import boto3
            except ImportError as exc:
                raise SystemExit("boto3 is required in the Kubernetes image to upload dbt docs to S3.") from exc

            target_dir = Path({json.dumps(docs_target)})
            bucket_name = {json.dumps(self.bucket_name)}
            folder_dir = {json.dumps(folder_dir)}

            client_kwargs = dict()
            endpoint_url = os.environ.get("AWS_ENDPOINT_URL_S3")
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url

            client_config = json.loads(os.environ.get("COSMOS_AWS_CLIENT_CONFIG", "{{}}"))
            if "verify" in client_config:
                client_kwargs["verify"] = client_config["verify"]

            config_kwargs = client_config.get("config_kwargs")
            if config_kwargs:
                from botocore.config import Config

                client_kwargs["config"] = Config(**config_kwargs)

            s3 = boto3.client("s3", **client_kwargs)
            for file_path in target_dir.rglob("*"):
                if not file_path.is_file():
                    continue

                relative_path = file_path.relative_to(target_dir).as_posix()
                key = f"{{folder_dir}}/{{relative_path}}" if folder_dir else relative_path
                content_type, _ = mimetypes.guess_type(str(file_path))
                extra_args = {{"ContentType": content_type}} if content_type else None
                print(f"Uploading {{file_path}} to s3://{{bucket_name}}/{{key}}")
                if extra_args:
                    s3.upload_file(str(file_path), bucket_name, key, ExtraArgs=extra_args)
                else:
                    s3.upload_file(str(file_path), bucket_name, key)
            """).strip()
        return self._run_python_script_command(upload_script)

    def get_upload_env_vars(self) -> dict[str, str]:
        return self.aws_env_vars_from_connection(self.connection_id)

    def aws_env_vars_from_connection(self, connection_id: str) -> dict[str, str]:
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            from cosmos.operators.lazy_load import MissingPackage

            AwsBaseHook = MissingPackage(
                "airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook",
                "amazon",
            )

        hook = AwsBaseHook(aws_conn_id=connection_id, client_type="s3")
        conn_config = hook.conn_config
        conn_extra = conn_config.extra_config

        config_kwargs = conn_extra.get("config_kwargs") or {}

        region_name = hook.region_name or conn_extra.get("region") or config_kwargs.get("region_name")
        endpoint_url = conn_config.get_service_endpoint_url("s3")
        verify = hook.verify

        env_vars = {}
        if conn_config.aws_access_key_id:
            env_vars["AWS_ACCESS_KEY_ID"] = conn_config.aws_access_key_id
        if conn_config.aws_secret_access_key:
            env_vars["AWS_SECRET_ACCESS_KEY"] = conn_config.aws_secret_access_key
        if conn_config.aws_session_token:
            env_vars["AWS_SESSION_TOKEN"] = conn_config.aws_session_token
        if endpoint_url:
            env_vars["AWS_ENDPOINT_URL_S3"] = endpoint_url
        if region_name:
            env_vars["AWS_DEFAULT_REGION"] = region_name
            env_vars["AWS_REGION"] = region_name

        client_config = {}
        if verify is not None:
            client_config["verify"] = verify
        if config_kwargs:
            client_config["config_kwargs"] = config_kwargs
        if client_config:
            env_vars["COSMOS_AWS_CLIENT_CONFIG"] = json.dumps(client_config)

        return env_vars


class DbtDocsGCSKubernetesOperator(DbtDocsCloudKubernetesOperator):
    """
    Executes `dbt docs generate` inside a Kubernetes Pod and uploads the generated
    documentation to Google Cloud Storage also inside that Pod using ``google-cloud-storage``.

    The Pod authenticates with Application Default Credentials (for example GKE Workload Identity) unless
    ``connection_id`` is given. With ``connection_id``, the service account key (``keyfile_dict`` or ``key_path``) and
    the project of that Airflow connection are resolved on the Airflow worker and passed to the Pod as environment
    variables.

    :param bucket_name: Google Cloud Storage's bucket name
    :param connection_id: Google Cloud's Airflow connection ID. Optional: without it the Pod uses its own
        Application Default Credentials.
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#4772d5"

    # Connection fields that hold credentials the Pod cannot use. Ignoring them would make the Pod silently upload
    # with its own identity instead of the one configured in the connection.
    _UNSUPPORTED_CREDENTIAL_FIELDS = (
        "key_secret_name",
        "credential_config_file",
        "idp_issuer_url",
        "is_anonymous",
        "impersonation_chain",
    )

    def __init__(
        self,
        bucket_name: str,
        connection_id: str | None = None,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir

    def build_upload_shell_command(self, docs_target: str) -> str:
        folder_dir = self.folder_dir.strip("/") if self.folder_dir else ""
        upload_script = textwrap.dedent(f"""
            import json
            import mimetypes
            import os
            from pathlib import Path

            try:
                from google.cloud import storage
            except ImportError as exc:
                raise SystemExit(
                    "google-cloud-storage is required in the Kubernetes image to upload dbt docs to GCS."
                ) from exc

            target_dir = Path({json.dumps(docs_target)})
            bucket_name = {json.dumps(self.bucket_name)}
            folder_dir = {json.dumps(folder_dir)}

            # Uploading objects needs no project. An explicit None stops the client from raising when it cannot
            # find one, for example with user credentials outside Google Cloud.
            client_kwargs = dict(project=os.environ.get("GOOGLE_CLOUD_PROJECT") or None)

            keyfile_dict = os.environ.get("COSMOS_GCP_KEYFILE_DICT")
            if keyfile_dict:
                from google.oauth2 import service_account

                client_kwargs["credentials"] = service_account.Credentials.from_service_account_info(
                    json.loads(keyfile_dict)
                )

            bucket = storage.Client(**client_kwargs).bucket(bucket_name)
            for file_path in target_dir.rglob("*"):
                if not file_path.is_file():
                    continue

                relative_path = file_path.relative_to(target_dir).as_posix()
                blob_name = f"{{folder_dir}}/{{relative_path}}" if folder_dir else relative_path
                content_type, _ = mimetypes.guess_type(str(file_path))
                print(f"Uploading {{file_path}} to gs://{{bucket_name}}/{{blob_name}}")
                bucket.blob(blob_name).upload_from_filename(str(file_path), content_type=content_type)
            """).strip()
        return self._run_python_script_command(upload_script)

    def get_upload_env_vars(self) -> dict[str, str]:
        if not self.connection_id:
            self.log.info(
                "No connection_id given: the Pod uploads the dbt docs with its Application Default Credentials"
            )
            return {}
        return self.gcp_env_vars_from_connection(self.connection_id)

    def gcp_env_vars_from_connection(self, connection_id: str) -> dict[str, str]:
        try:
            from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
        except ImportError:
            from cosmos.operators.lazy_load import MissingPackage

            GoogleBaseHook = MissingPackage(
                "airflow.providers.google.common.hooks.base_google.GoogleBaseHook",
                "google",
            )

        hook = GoogleBaseHook(gcp_conn_id=connection_id)

        unsupported_fields = [field for field in self._UNSUPPORTED_CREDENTIAL_FIELDS if hook._get_field(field)]
        if unsupported_fields:
            raise CosmosValueError(
                f"The Airflow connection '{connection_id}' sets {', '.join(unsupported_fields)}, which "
                f"{self.__class__.__name__} cannot pass to the Kubernetes Pod. Use a connection with `keyfile_dict` "
                "or `key_path`, or leave `connection_id` unset so the Pod uses its Application Default Credentials."
            )

        env_vars = {}
        keyfile_dict = hook._get_field("keyfile_dict")
        key_path = hook._get_field("key_path")
        if keyfile_dict:
            env_vars["COSMOS_GCP_KEYFILE_DICT"] = (
                keyfile_dict if isinstance(keyfile_dict, str) else json.dumps(keyfile_dict)
            )
        elif key_path:
            # The key file lives on the Airflow worker, not in the Pod, so its content is passed instead of its path.
            env_vars["COSMOS_GCP_KEYFILE_DICT"] = Path(key_path).read_text(encoding="utf-8")
        else:
            self.log.info(
                "The Airflow connection '%s' has no service account key: the Pod uploads the dbt docs with its "
                "Application Default Credentials",
                connection_id,
            )

        project_id = hook._get_field("project")
        if project_id:
            env_vars["GOOGLE_CLOUD_PROJECT"] = project_id

        return env_vars
