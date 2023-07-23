"Contains the local operators to run dbt commands locally."
from __future__ import annotations

import logging
import os
import shutil
import signal
import tempfile
from pathlib import Path
from typing import Callable, Optional, Any

import yaml
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm import Session

from cosmos.operators.base import DbtBaseOperator
from cosmos.hooks.subprocess import (
    FullOutputSubprocessHook,
    FullOutputSubprocessResult,
)
from cosmos.dbt.parser.output import (
    extract_log_issues,
    parse_output,
)

logger = logging.getLogger(__name__)


class DbtLocalBaseOperator(DbtBaseOperator):
    """
    Executes a dbt core cli command locally.

    :param install_deps: If true, install dependencies before running the command
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    :param should_store_compiled_sql: If true, store the compiled SQL in the compiled_sql rendered template.
    """

    template_fields: list[str] = DbtBaseOperator.template_fields + ["compiled_sql"]
    template_fields_renderers = {
        "compiled_sql": "sql",
    }

    def __init__(
        self,
        callback: Optional[Callable[[str], None]] = None,
        should_store_compiled_sql: bool = True,
        **kwargs: Any,
    ) -> None:
        self.callback = callback
        self.compiled_sql = ""
        self.should_store_compiled_sql = should_store_compiled_sql
        super().__init__(**kwargs)

    @cached_property
    def subprocess_hook(self) -> FullOutputSubprocessHook:
        """Returns hook for running the bash command."""
        return FullOutputSubprocessHook()

    def exception_handling(self, _: Context, result: FullOutputSubprocessResult) -> None:
        "If the exit code is the skip exit code, raise an AirflowSkipException. Otherwise, raise an AirflowException."
        skip_exit_code = self.cosmos_config.execution_config.skip_exit_code
        exit_code = result.exit_code

        if skip_exit_code is not None and exit_code == skip_exit_code:
            raise AirflowSkipException(f"dbt command returned exit code {skip_exit_code}. Skipping.")

        if exit_code != 0:
            raise AirflowException(
                f"dbt command failed. The command returned a non-zero exit code {exit_code}. Details: ",
                *result.full_output,
            )

    @provide_session
    def store_compiled_sql(self, tmp_project_dir: str, context: Context, session: Session = NEW_SESSION) -> None:
        """
        Takes the compiled SQL files from the dbt run and stores them in the compiled_sql rendered template.
        Gets called after every dbt run.
        """
        if not self.should_store_compiled_sql:
            return

        compiled_queries = {}
        # dbt compiles sql files and stores them in the target directory
        for folder_path, _, file_paths in os.walk(os.path.join(tmp_project_dir, "target")):
            for file_path in file_paths:
                if not file_path.endswith(".sql"):
                    continue

                compiled_sql_path = Path(os.path.join(folder_path, file_path))
                compiled_sql = compiled_sql_path.read_text(encoding="utf-8")

                relative_path = str(compiled_sql_path.relative_to(tmp_project_dir))
                compiled_queries[relative_path] = compiled_sql.strip()

        for name, query in compiled_queries.items():
            self.compiled_sql += f"-- {name}\n{query}\n\n"

        self.compiled_sql = self.compiled_sql.strip()

        # need to refresh the rendered task field record in the db because Airflow only does this
        # before executing the task, not after

        task_instance = context["ti"]
        task_instance.task.template_fields = self.template_fields
        rtif = RenderedTaskInstanceFields(task_instance, render_templates=False)

        # delete the old records
        session.query(RenderedTaskInstanceFields).filter(
            RenderedTaskInstanceFields.dag_id == self.dag_id,
            RenderedTaskInstanceFields.task_id == self.task_id,
            RenderedTaskInstanceFields.run_id == task_instance.run_id,
        ).delete()
        session.add(rtif)

    def execute(self, context: Context) -> None:
        """
        Copies the dbt project to a temporary directory and runs the command.
        """
        env = self.get_env(context=context)
        project_dir = self.cosmos_config.project_config.dbt_project_path
        dbt_executable_path = str(self.cosmos_config.execution_config.dbt_executable_path)

        with tempfile.TemporaryDirectory() as tmp_dir:
            logger.info(
                "Cloning project to writable temp directory %s from %s",
                tmp_dir,
                project_dir,
            )

            # need a subfolder because shutil.copytree will fail if the destination dir already exists
            tmp_project_dir = os.path.join(tmp_dir, "dbt_project")
            shutil.copytree(
                project_dir,
                tmp_project_dir,
            )

            desired_profile_path = Path(tmp_dir) / "profiles.yml"
            with self.cosmos_config.profile_config.ensure_profile(desired_profile_path=desired_profile_path) as (
                profiles_yml_path,
                profile_env_vars,
            ):
                env.update(profile_env_vars)

                if self.cosmos_config.execution_config.install_deps:
                    logger.info(
                        "Trying to run the command:\n %s\nFrom %s", [dbt_executable_path, "deps"], tmp_project_dir
                    )
                    self.subprocess_hook.run_command(
                        command=[dbt_executable_path, "deps"],
                        env=env,
                        cwd=tmp_project_dir,
                    )

                generated_cmd = self.build_cmd(
                    [
                        "--profiles-dir",
                        os.path.dirname(profiles_yml_path),
                        "--profile",
                        self.cosmos_config.profile_config.profile_name,
                        "--target",
                        self.cosmos_config.profile_config.target_name,
                    ]
                )

                logger.info("Trying to run the command:\n %s\nFrom %s", generated_cmd, tmp_project_dir)

                result = self.subprocess_hook.run_command(
                    command=generated_cmd,
                    env=env,
                    cwd=tmp_project_dir,
                )

                self.store_compiled_sql(tmp_project_dir, context)
                self.exception_handling(context, result)

                if self.callback:
                    self.callback(tmp_project_dir)

    def on_kill(self) -> None:
        "Overrides the default on_kill behavior to send a SIGINT signal to the process group."
        if self.cosmos_config.execution_config.cancel_query_on_kill:
            self.subprocess_hook.log.info("Sending SIGINT signal to process group")
            if self.subprocess_hook.sub_process and hasattr(self.subprocess_hook.sub_process, "pid"):
                os.killpg(os.getpgid(self.subprocess_hook.sub_process.pid), signal.SIGINT)
        else:
            self.subprocess_hook.send_sigterm()


class DbtLSLocalOperator(DbtLocalBaseOperator):
    "Executes a dbt core ls command."

    ui_color = "#DBCDF6"
    base_cmd = ["ls"]


class DbtSeedLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"
    base_cmd = ["seed"]

    def __init__(self, full_refresh: bool = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def build_cmd(self, flags: list[str] | None = None) -> list[str]:
        "Overrides the base class build_cmd to add the full-refresh flag."
        cmd = super().build_cmd(flags=flags)

        if self.full_refresh is True:
            cmd.append("--full-refresh")

        return cmd


class DbtSnapshotLocalOperator(DbtLocalBaseOperator):
    "Executes a dbt core snapshot command."

    ui_color = "#964B00"
    base_cmd = ["snapshot"]


class DbtRunLocalOperator(DbtLocalBaseOperator):
    "Executes a dbt core run command."

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"
    base_cmd = ["run"]


class DbtTestLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core test command.

    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    ui_color = "#8194E0"
    base_cmd = ["test"]

    def __init__(
        self,
        on_warning_callback: Callable[[dict[str, object]], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.on_warning_callback = on_warning_callback

    def exception_handling(self, context: Context, result: FullOutputSubprocessResult) -> None:
        "Checks for warnings and calls the on_warning_callback if there are any."
        super().exception_handling(context, result)

        if not self.on_warning_callback or "Nothing to do" in result.output:
            return

        warnings = parse_output(result, "WARN")
        if warnings > 0:
            test_names, test_results = extract_log_issues(result.full_output)

            warning_context = dict(context)
            warning_context["test_names"] = test_names
            warning_context["test_results"] = test_results

            self.on_warning_callback(warning_context)


class DbtRunOperationLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields = DbtLocalBaseOperator.template_fields + ["args"]

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)
        self.base_cmd = ["run-operation", macro_name]

    def build_cmd(self, flags: list[str] | None = None) -> list[str]:
        "Overrides the base class build_cmd to add the args flag."
        cmd = super().build_cmd(flags=flags)

        if self.args is not None:
            cmd.append("--args")
            cmd.append(yaml.dump(self.args))

        return cmd


class DbtDocsLocalOperator(DbtLocalBaseOperator):
    """
    Executes `dbt docs generate` command.
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

    ui_color = "#8194E0"
    base_cmd = ["docs", "generate"]
    required_files = ["index.html", "manifest.json", "graph.gpickle", "catalog.json"]


class DbtDocsS3LocalOperator(DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command and upload to S3 storage. Returns the S3 path to the generated documentation.

    :param aws_conn_id: S3's Airflow connection ID
    :param bucket_name: S3's bucket name
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#FF9900"

    def __init__(
        self,
        aws_conn_id: str,
        bucket_name: str,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        "Initializes the operator."
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir

        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.upload_to_s3

    def upload_to_s3(self, project_dir: str) -> None:
        "Uploads the generated documentation to S3."
        logger.info(
            'Attempting to upload generated docs to S3 using S3Hook("%s")',
            self.aws_conn_id,
        )

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        target_dir = f"{project_dir}/target"

        hook = S3Hook(
            self.aws_conn_id,
            extra_args={
                "ContentType": "text/html",
            },
        )

        for filename in self.required_files:
            logger.info("Uploading %s to %s", filename, f"s3://{self.bucket_name}/{filename}")

            key = f"{self.folder_dir}/{filename}" if self.folder_dir else filename

            hook.load_file(
                filename=f"{target_dir}/{filename}",
                bucket_name=self.bucket_name,
                key=key,
                replace=True,
            )


class DbtDocsAzureStorageLocalOperator(DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command and upload to Azure Blob Storage.

    :param azure_conn_id: Azure Blob Storage's Airflow connection ID
    :param container_name: Azure Blob Storage's bucket name
    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#007FFF"

    def __init__(
        self,
        azure_conn_id: str,
        container_name: str,
        folder_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        "Initializes the operator."
        self.azure_conn_id = azure_conn_id
        self.container_name = container_name
        self.folder_dir = folder_dir

        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.upload_to_azure

    def upload_to_azure(self, project_dir: str) -> None:
        "Uploads the generated documentation to Azure Blob Storage."
        logger.info(
            'Attempting to upload generated docs to Azure Blob Storage using WasbHook(conn_id="%s")',
            self.azure_conn_id,
        )

        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        target_dir = f"{project_dir}/target"

        hook = WasbHook(
            self.azure_conn_id,
        )

        for filename in self.required_files:
            logger.info(
                "Uploading %s to %s",
                filename,
                f"wasb://{self.container_name}/{filename}",
            )

            blob_name = f"{self.folder_dir}/{filename}" if self.folder_dir else filename

            hook.load_file(
                file_path=f"{target_dir}/{filename}",
                container_name=self.container_name,
                blob_name=blob_name,
                overwrite=True,
            )
