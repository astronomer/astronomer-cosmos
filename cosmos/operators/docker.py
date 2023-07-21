from __future__ import annotations

import logging
from typing import Any, Callable, Sequence

import yaml
from airflow.utils.context import Context

from cosmos.operators.base import DbtBaseOperator

logger = logging.getLogger(__name__)

# docker is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    raise ImportError(
        "Could not import DockerOperator. Ensure you've installed the docker provider separately or "
        "with with `pip install astronomer-cosmos[...,docker]`."
    )


class DbtDockerBaseOperator(DockerOperator, DbtBaseOperator):  # type: ignore[misc] # ignores subclass MyPy error
    """
    Executes a dbt core cli command in a Docker container.

    """

    template_fields: Sequence[str] = DbtBaseOperator.template_fields + DockerOperator.template_fields

    intercept_flag = False

    def __init__(
        self,
        image: str,  # Make image a required argument since it's required by DockerOperator
        **kwargs: Any,
    ) -> None:
        super().__init__(image=image, **kwargs)

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> Any:
        self.build_command(cmd_flags, context)
        self.log.info(f"Running command: {self.command}")  # type: ignore[has-type]
        return super().execute(context)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        # set env vars
        self.environment = {**env_vars, **self.environment}  # type: ignore[has-type]
        self.command = dbt_cmd


class DbtLSDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["ls"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtSeedDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"

    def __init__(self, full_refresh: bool = False, **kwargs: str) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = ["seed"]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context) -> Any:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtSnapshotDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core snapshot command.

    """

    ui_color = "#964B00"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["snapshot"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtRunDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["run"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtTestDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core test command.
    """

    ui_color = "#8194E0"

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["test"]
        # as of now, on_warning_callback in docker executor does nothing
        self.on_warning_callback = on_warning_callback

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtRunOperationDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = ("args",)

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: str) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)
        self.base_cmd = ["run-operation", macro_name]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.args is not None:
            flags.append("--args")
            flags.append(yaml.dump(self.args))
        return flags

    def execute(self, context: Context) -> Any:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
