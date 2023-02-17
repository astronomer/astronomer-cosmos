from __future__ import annotations

from typing import Sequence

import yaml
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.context import Context

from cosmos.providers.dbt.core.operators import DbtBaseOperator


class DbtDockerBaseOperator(DockerOperator, DbtBaseOperator):
    """
    Executes a dbt core cli command in a Docker container.

    """

    template_fields: Sequence[str] = (
        DbtBaseOperator.template_fields + DockerOperator.template_fields
    )

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def build_cmd_and_args(self, env: dict = {}, cmd_flags: list = None):
        dbt_cmd, env_vars = self.build_cmd(
            env=env, cmd_flags=cmd_flags, handle_profile=False
        )

        ## set env vars
        self.environment = {**env_vars, **self.environment}

        self.command = dbt_cmd
        self.log.info(f"Building command: {self.command}")


class DbtLSDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core ls command.

    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        self.build_cmd_and_args()
        return super().execute(context)


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
        self.build_cmd_and_args(cmd_flags=cmd_flags)
        return super().execute(context)


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
        self.build_cmd_and_args()
        return super().execute(context)


class DbtTestDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core test command.

    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        self.build_cmd_and_args()
        return super().execute(context)


class DbtRunOperationDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :type macro_name: str
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    :type args: dict
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
        self.build_cmd_and_args(cmd_flags=cmd_flags)
        return super().execute(context)


class DbtDepsDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core deps command.

    :param vars: Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file
    :type vars: dict
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "deps"

    def execute(self, context: Context):
        self.build_cmd_and_args()
        return super().execute(context)
