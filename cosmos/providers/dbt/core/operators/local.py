from __future__ import annotations

import fcntl
import logging
import os
import shutil
import signal
import time
from filecmp import dircmp
from pathlib import Path
from typing import Callable, Optional, Sequence

import yaml
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from filelock import FileLock

from cosmos.providers.dbt.constants import DBT_PROFILE_PATH
from cosmos.providers.dbt.core.operators.base import DbtBaseOperator
from cosmos.providers.dbt.core.utils.adapted_subprocesshook import SubprocessHook
from cosmos.providers.dbt.core.utils.file_syncing import (
    exclude,
    has_differences,
    is_file_locked,
)
from cosmos.providers.dbt.core.utils.warn_parsing import (
    extract_log_issues,
    parse_output,
)

logger = logging.getLogger(__name__)


class DbtLocalBaseOperator(DbtBaseOperator):
    """
    Executes a dbt core cli command locally.

    """

    template_fields: Sequence[str] = DbtBaseOperator.template_fields

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def exception_handling(self, result: SubprocessResult):
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(
                f"dbt command returned exit code {self.skip_exit_code}. Skipping."
            )
        elif result.exit_code != 0:
            raise AirflowException(
                f"dbt command failed. The command returned a non-zero exit code {result.exit_code}."
            )

    def run_command(
        self,
        cmd: list[str],
        env: dict[str, str],
    ) -> SubprocessResult:
        # check project_dir
        if self.project_dir is not None:
            if not os.path.exists(self.project_dir):
                raise AirflowException(
                    f"Can not find the project_dir: {self.project_dir}"
                )
            if not os.path.isdir(self.project_dir):
                raise AirflowException(
                    f"The project_dir {self.project_dir} must be a directory"
                )

        # routing the dbt project to a tmp dir for r/w operations
        target_dir = f"/tmp/dbt/{os.path.basename(self.project_dir)}"

        changes = True  # set changes to true by default
        if os.path.exists(target_dir):
            # if the directory doesn't exist or if there are changes -- keep changes as true
            comparison = dircmp(
                self.project_dir, target_dir, ignore=["logs", "target", ".lock"]
            )  # compares tmp and project dir
            changes = has_differences(comparison)  # check for changes

        # if there are changes between tmp and project_dir then copy them over
        if changes:
            logging.info(
                f"Changes detected - copying {self.project_dir} to {target_dir}"
            )
            # put a lock on the dest so that it isn't getting written multiple times
            lock_file = os.path.join(target_dir, ".lock")

            # if there is already a lock file - then just wait for it to be released and continue without copying
            if os.path.exists(lock_file) and is_file_locked(lock_file):
                while os.path.exists(lock_file):
                    with open(lock_file, "w") as lock_f:
                        try:
                            # Lock acquired, the lock file is available
                            fcntl.flock(lock_f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                            break
                        except OSError:
                            # Lock is held by another process, wait and try again
                            time.sleep(1)

                # The lock file is available, release the shared lock
                with open(lock_file, "w") as lock_f:
                    fcntl.flock(lock_f, fcntl.LOCK_UN)

            # otherwise create a lock file and copy the dbt directory
            else:
                os.makedirs(os.path.dirname(lock_file), exist_ok=True)
                lock_path = Path(target_dir) / ".lock"
                with FileLock(str(lock_path), timeout=15):
                    if os.path.exists(target_dir):
                        shutil.rmtree(target_dir)
                    shutil.copytree(self.project_dir, target_dir, ignore=exclude)
        else:
            logging.info(
                f"No differences detected between {self.project_dir} and {target_dir}"
            )

        # Fix the profile path, so it's not accidentally superseded by the end user.
        env["DBT_PROFILES_DIR"] = DBT_PROFILE_PATH.parent
        # run bash command
        result = self.subprocess_hook.run_command(
            command=cmd, env=env, output_encoding=self.output_encoding, cwd=target_dir
        )
        self.exception_handling(result)
        return result

    def build_and_run_cmd(
        self, context: Context, cmd_flags: list[str] | None = None
    ) -> SubprocessResult:
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.log.info(f"Running command: {dbt_cmd}")
        return self.run_command(cmd=dbt_cmd, env=env)

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

    def _should_run_tests(self, result: SubprocessResult) -> bool:
        """
        Check if any tests are defined to run in the DAG. If tests are defined
        and on_warning_callback is set, then function returns True.

        :param
            result (SubprocessResult): The output from the build and run command.
        """

        no_tests_message = "Nothing to do"
        return self.on_warning_callback and no_tests_message not in result.output

    def _handle_warnings(self, result: SubprocessResult, context: Context) -> None:
        """
        Handles warnings by extracting log issues, creating additional context, and calling the
        on_warning_callback with the updated context.

        Args:
            result: The result object from the build and run command.
            context (Context): The original airflow context in which the build and run command was executed.
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


class DbtDepsLocalOperator(DbtLocalBaseOperator):
    """
    Executes a dbt core deps command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "deps"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
        return result.output
