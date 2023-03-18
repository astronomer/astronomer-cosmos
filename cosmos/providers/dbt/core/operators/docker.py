from __future__ import annotations

import logging
from typing import Sequence

import yaml
from airflow.utils.context import Context
from cosmos.providers.dbt.core.operators.base import DbtBaseOperator
from airflow.providers.docker.operators.docker import DockerOperator

logger = logging.getLogger(__name__)


class DbtDockerBaseOperator(DockerOperator, DbtBaseOperator):
    """
    Executes a dbt core cli command in a Docker container.

    """

    template_fields: Sequence[str] = (
        DbtBaseOperator.template_fields + DockerOperator.template_fields
    )

    intercept_flag = False

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def build_and_run_cmd(
            self,
            context: Context,
            cmd_flags: list[str] | None = None):

        dbt_cmd, env_vars = self.build_cmd(
            context=context,
            cmd_flags=cmd_flags,
            handle_profile=False
        )

        # set env vars
        self.environment = {**env_vars, **self.environment}

        self.command = dbt_cmd
        self.log.info(f"Running command: {self.command}")
        return super().execute(context)


class DbtLSDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        return self.build_and_run_cmd(context=context)


class DbtSeedDockerOperator(DbtDockerBaseOperator):
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
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtRunDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "run"

    def execute(self, context: Context):
        return self.build_and_run_cmd(context=context)


class DbtTestDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core test command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        return self.build_and_run_cmd(context=context)


class DbtRunOperationDockerOperator(DbtDockerBaseOperator):
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
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtDepsDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core deps command.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "deps"

    def execute(self, context: Context):
        return self.build_and_run_cmd(context=context)
