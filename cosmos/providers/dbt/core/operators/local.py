from __future__ import annotations

import logging
import os
import shutil
import signal
import tempfile
from typing import Sequence

import yaml
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult
from airflow.utils.context import Context

from cosmos.providers.dbt.core.operators.base import DbtBaseOperator

logger = logging.getLogger(__name__)


class DbtLocalBaseOperator(DbtBaseOperator):
    """
    Executes a dbt core cli command locally.

    :param install_deps: If true, install dependencies before running the command
    """

    template_fields: Sequence[str] = DbtBaseOperator.template_fields

    def __init__(
        self,
        install_deps: bool = False,
        **kwargs,
    ) -> None:
        self.install_deps = install_deps
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

            result = self.subprocess_hook.run_command(
                command=cmd,
                env=env,
                output_encoding=self.output_encoding,
                cwd=tmp_project_dir,
            )

            self.exception_handling(result)

            return result

    def build_and_run_cmd(
        self, context: Context, cmd_flags: list[str] | None = None
    ) -> SubprocessResult:
        dbt_cmd, env = self.build_cmd(context=context, cmd_flags=cmd_flags)
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
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(context=context)
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
