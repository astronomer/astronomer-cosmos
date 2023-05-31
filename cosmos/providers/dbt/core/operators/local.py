from __future__ import annotations

import logging
import os
import shutil
import signal
import tempfile
from collections import namedtuple
from pathlib import Path
from typing import Callable, Optional, Sequence

import yaml
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm import Session

from cosmos.providers.dbt.core.operators.base import DbtBaseOperator
from cosmos.providers.dbt.core.utils.adapted_subprocesshook import (
    FullOutputSubprocessHook,
)
from cosmos.providers.dbt.core.utils.warn_parsing import (
    extract_log_issues,
    parse_output,
)

logger = logging.getLogger(__name__)

FullOutputSubprocessResult = namedtuple("FullOutputSubprocessResult", ["exit_code", "output", "full_output"])


class DbtLocalBaseOperator(DbtBaseOperator):
    """
    Executes a dbt core cli command locally.

    :param install_deps: If true, install dependencies before running the command
    :param callback: A callback function called on after a dbt run with a path to the dbt project directory.
    """

    template_fields: Sequence[str] = DbtBaseOperator.template_fields + ("compiled_sql",)
    template_fields_renderers = {
        "compiled_sql": "sql",
    }

    def __init__(
        self,
        install_deps: bool = False,
        callback: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> None:
        self.install_deps = install_deps
        self.callback = callback
        self.compiled_sql = ""
        super().__init__(**kwargs)

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return FullOutputSubprocessHook()

    def exception_handling(self, result: FullOutputSubprocessResult):
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"dbt command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            raise AirflowException(
                f"dbt command failed. The command returned a non-zero exit code {result.exit_code}. Details: ",
                *result.full_output,
            )

    @provide_session
    def store_compiled_sql(self, tmp_project_dir: str, context: Context, session: Session = NEW_SESSION) -> None:
        """
        Takes the compiled SQL files from the dbt run and stores them in the compiled_sql rendered template.
        Gets called after every dbt run.
        """
        compiled_queries = {}
        # dbt compiles sql files and stores them in the target directory
        for root, _, files in os.walk(os.path.join(tmp_project_dir, "target")):
            for file in files:
                if not file.endswith(".sql"):
                    continue

                compiled_sql_path = Path(os.path.join(root, file))
                compiled_sql = compiled_sql_path.read_text(encoding="utf-8")
                compiled_queries[file] = compiled_sql.strip()

        for name, query in compiled_queries.items():
            self.compiled_sql += f"-- {name}\n{query}\n\n"

        self.compiled_sql = self.compiled_sql.strip()

        # need to refresh the rendered task field record in the db because Airflow only does this
        # before executing the task, not after
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        ti = context["ti"]
        ti.task.template_fields = self.template_fields
        rtif = RenderedTaskInstanceFields(ti, render_templates=False)

        # delete the old records
        session.query(RenderedTaskInstanceFields).filter(
            RenderedTaskInstanceFields.dag_id == self.dag_id,
            RenderedTaskInstanceFields.task_id == self.task_id,
            RenderedTaskInstanceFields.run_id == ti.run_id,
        ).delete()
        session.add(rtif)

    def handle_profiles_yml(self, project_dir: str) -> None:
        """
        If there's a profiles.yml file in the project dir, remove it. Cosmos doesn't support
        user-supplied profiles.yml files.
        """
        profiles_path = os.path.join(project_dir, "profiles.yml")
        if os.path.exists(profiles_path):
            logger.warning(
                "Cosmos doesn't support user-supplied profiles.yml files. \
                    Please use Airflow connections instead. \
                    Ignoring profiles.yml…"
            )
            os.remove(profiles_path)

    def run_command(
        self,
        cmd: list[str],
        env: dict[str, str],
        context: Context,
    ) -> FullOutputSubprocessResult:
        """
        Copies the dbt project to a temporary directory and runs the command.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            logger.info(
                "Cloning project to writable temp directory %s from %s",
                tmp_dir,
                self.project_dir,
            )

            # need a subfolder because shutil.copytree will fail if the destination dir already exists
            tmp_project_dir = os.path.join(tmp_dir, "dbt_project")
            shutil.copytree(
                self.project_dir,
                tmp_project_dir,
            )

            self.handle_profiles_yml(tmp_project_dir)

            # if we need to install deps, do so
            if self.install_deps:
                self.subprocess_hook.run_command(
                    command=[self.dbt_executable_path, "deps"],
                    env=env,
                    output_encoding=self.output_encoding,
                    cwd=tmp_project_dir,
                )

            logger.info(f"Trying to run the command:\n {cmd}\nFrom {tmp_project_dir}")

            result = self.subprocess_hook.run_command(
                command=cmd,
                env=env,
                output_encoding=self.output_encoding,
                cwd=tmp_project_dir,
            )

            self.exception_handling(result)
            self.store_compiled_sql(tmp_project_dir, context)
            if self.callback:
                self.callback(tmp_project_dir)

            return result

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> FullOutputSubprocessResult:
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        return self.run_command(cmd=dbt_cmd, env=env, context=context)

    def execute(self, context: Context) -> str:
        # TODO is this going to put loads of unnecessary stuff in to xcom?
        return self.build_and_run_cmd(context=context).output

    def on_kill(self) -> None:
        if self.cancel_query_on_kill:
            self.subprocess_hook.log.info("Sending SIGINT signal to process group")
            if self.subprocess_hook.sub_process and hasattr(self.subprocess_hook.sub_process, "pid"):
                os.killpg(os.getpgid(self.subprocess_hook.sub_process.pid), signal.SIGINT)
        else:
            self.subprocess_hook.send_sigterm()


class DbtLSLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output


class DbtSeedLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"

    def __init__(self, full_refresh: bool = False, **kwargs) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = "seed"

    def add_cmd_flags(self):
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context):
        cmd_flags = self.add_cmd_flags()
        result = self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
        return result.output


class DbtSnapshotLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core snapshot command.

    """

    ui_color = "#964B00"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "snapshot"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output


class DbtRunLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "run"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output


class DbtTestLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core test command.
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    ui_color = "#8194E0"

    def __init__(
        self,
        on_warning_callback: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"
        self.on_warning_callback = on_warning_callback

    def _should_run_tests(
        self,
        result: FullOutputSubprocessResult,
        no_tests_message: str = "Nothing to do",
    ) -> bool:
        """
        Check if any tests are defined to run in the DAG. If tests are defined
        and on_warning_callback is set, then function returns True.

        :param result: The output from the build and run command.
        """

        return self.on_warning_callback and no_tests_message not in result.output

    def _handle_warnings(self, result: FullOutputSubprocessResult, context: Context) -> None:
        """
         Handles warnings by extracting log issues, creating additional context, and calling the
         on_warning_callback with the updated context.

        :param result: The result object from the build and run command.
        :param context: The original airflow context in which the build and run command was executed.
        """
        test_names, test_results = extract_log_issues(result.full_output)

        warning_context = dict(context)
        warning_context["test_names"] = test_names
        warning_context["test_results"] = test_results

        self.on_warning_callback(warning_context)

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)

        if not self._should_run_tests(result):
            return result.output

        warnings = parse_output(result, "WARN")
        if warnings > 0:
            self._handle_warnings(result, context)

        return result.output


class DbtRunOperationLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = "args"

    def __init__(self, macro_name: str, args: dict = None, **kwargs) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)
        self.base_cmd = ["run-operation", macro_name]

    def add_cmd_flags(self):
        flags = []
        if self.args is not None:
            flags.append("--args")
            flags.append(yaml.dump(self.args))
        return flags

    def execute(self, context: Context):
        cmd_flags = self.add_cmd_flags()
        result = self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
        return result.output


class DbtDocsLocalOperator(DbtLocalBaseOperator):
    """
    Executes `dbt docs generate` command.
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

    ui_color = "#8194E0"

    required_files = ["index.html", "manifest.json", "graph.gpickle", "catalog.json"]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["docs", "generate"]

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output


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
        **kwargs,
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
        **kwargs,
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


class DbtDepsLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core deps command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        raise DeprecationWarning(
            "The DbtDepsOperator has been deprecated. " "Please use the `install_deps` flag in dbt_args instead."
        )
