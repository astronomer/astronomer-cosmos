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

FullOutputSubprocessResult = namedtuple(
    "FullOutputSubprocessResult", ["exit_code", "output", "full_output"]
)


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
            raise AirflowSkipException(
                f"dbt command returned exit code {self.skip_exit_code}. Skipping."
            )
        elif result.exit_code != 0:
            raise AirflowException(
                f"dbt command failed. The command returned a non-zero exit code {result.exit_code}. Details: ",
                *result.full_output,
            )

    @provide_session
    def store_compiled_sql(
        self, tmp_project_dir: str, context: Context, session: Session = NEW_SESSION
    ) -> None:
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
            # need a subfolder because shutil.copytree will fail if the destination dir already exists
            tmp_project_dir = os.path.join(tmp_dir, "dbt_project")
            shutil.copytree(
                self.project_dir,
                tmp_project_dir,
            )

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

    def build_and_run_cmd(
        self, context: Context, cmd_flags: list[str] | None = None
    ) -> FullOutputSubprocessResult:
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        return self.run_command(cmd=dbt_cmd, env=env, context=context)

    def execute(self, context: Context) -> str:
        # TODO is this going to put loads of unnecessary stuff in to xcom?
        return self.build_and_run_cmd(context=context).output

    def on_kill(self) -> None:
        if self.cancel_query_on_kill:
            self.subprocess_hook.log.info("Sending SIGINT signal to process group")
            if self.subprocess_hook.sub_process and hasattr(
                self.subprocess_hook.sub_process, "pid"
            ):
                os.killpg(
                    os.getpgid(self.subprocess_hook.sub_process.pid), signal.SIGINT
                )
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

    def _handle_warnings(
        self, result: FullOutputSubprocessResult, context: Context
    ) -> None:
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

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["docs", "generate"]

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output


class DbtDocsS3LocalOperator(DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command and upload to S3 storage.
    """

    ui_color = "#FF9900"

    def __init__(
        self, target_conn_id: str, bucket_name: str, folder_dir: str = None, **kwargs
    ) -> None:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self.target_conn_id = target_conn_id
        self.bucket_name = bucket_name
        self.folder_dir = folder_dir
        super().__init__(**kwargs)
        self.callback = self.upload_to_s3
        self.s3_hook = S3Hook(
            aws_conn_id=self.target_conn_id, extra_args={"ContentType": "text/html"}
        )
        if self.s3_hook.test_connection()[0] is not True:
            logger.error("ERROR: Failed to connect S3")
            raise AirflowException("Failed to connect S3")

    def upload_to_s3(self, tmp_project_dir: str) -> None:
        try:
            target_dir = f"{tmp_project_dir}/target"

            # iterate over the files in the target dir and upload them to S3
            for dirpath, _, filenames in os.walk(target_dir):
                for filename in filenames:
                    if self.folder_dir is not None:
                        key_path = f"{self.folder_dir}/{filename}"
                    else:
                        key_path = filename
                    self.s3_hook.load_file(
                        filename=f"{dirpath}/{filename}",
                        bucket_name=self.bucket_name,
                        key=key_path,
                        replace=True,
                    )
        except ImportError:
            logger.error("ERROR: the S3Hook isn't installed")
        # if there's a botocore.exceptions.NoCredentialsError, print a warning and just copy the docs locally
        except Exception as exc:
            if "NoCredentialsError" in str(exc):
                logger.error(
                    "ERROR: No AWS credentials found.\
                    To upload docs to S3, install the S3Hook and configure an S3 connection."
                )
            else:
                logger.error("ERROR: " + str(exc))

        return


class DbtDocsAzureStorageLocalOperator(DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command and upload to Azure Blob Storage.
    """

    ui_color = "#007FFF"

    def __init__(
        self, target_conn_id: str, container_name: str, folder_dir: str = None, **kwargs
    ) -> None:
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        self.target_conn_id = target_conn_id
        self.container_name = container_name
        self.folder_dir = folder_dir
        super().__init__(**kwargs)
        self.callback = self.upload_to_azure_storage
        self.azure_hook = WasbHook(wasb_conn_id=self.target_conn_id)
        if self.azure_hook.test_connection()[0] is not True:
            logger.error("ERROR: Failed to connect Azure Blob Storage")
            raise AirflowException("Failed to connect Azure Blob Storage")

    def upload_to_azure_storage(self, tmp_project_dir: str) -> None:
        try:
            from azure.storage.blob import ContentSettings

            target_dir = f"{tmp_project_dir}/target"
            keywords = {
                "overwrite": True,
                "content_settings": ContentSettings(content_type="text/html"),
            }

            # iterate over the files in the target dir and upload them to S3
            for dirpath, _, filenames in os.walk(target_dir):
                for filename in filenames:
                    if self.folder_dir is not None:
                        key_path = f"{self.folder_dir}/{filename}"
                    else:
                        key_path = filename
                    self.azure_hook.load_file(
                        file_path=f"{dirpath}/{filename}",
                        container_name=self.container_name,
                        blob_name=key_path,
                        **keywords,
                    )
        # if there's a botocore.exceptions.NoCredentialsError, print a warning and just copy the docs locally
        except Exception as exc:
            if "NoCredentialsError" in str(exc):
                logger.error(
                    "ERROR: No Azure credentials found.\
                    To upload docs to Azure Blob Storage, install the WasbHook and configure a connection."
                )
            else:
                logger.error("ERROR: " + str(exc))

        return


class DbtDepsLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core deps command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        raise DeprecationWarning(
            "The DbtDepsOperator has been deprecated. "
            "Please use the `install_deps` flag in dbt_args instead."
        )
