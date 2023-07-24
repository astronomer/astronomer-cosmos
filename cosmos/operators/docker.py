"Docker operators for dbt commands"
from __future__ import annotations

import logging
from typing import Any

import yaml
from airflow.utils.context import Context

from cosmos.operators.base import DbtBaseOperator

logger = logging.getLogger(__name__)

# docker is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError as exception:
    raise ImportError(
        "Could not import DockerOperator. Ensure you've installed the docker provider separately or"
        " with with `pip install astronomer-cosmos[...,docker]`."
    ) from exception


class DbtDockerBaseOperator(DockerOperator, DbtBaseOperator):  # type: ignore[misc] # ignores subclass MyPy error
    """
    Executes a dbt core cli command in a Docker container.
    """

    template_fields: list[str] = DbtBaseOperator.template_fields + list(DockerOperator.template_fields)

    def __init__(
        self,
        image: str,  # Make image a required argument since it's required by DockerOperator
        **kwargs: Any,
    ) -> None:
        super().__init__(image=image, **kwargs)

    def prepare(self, context: Context) -> None:
        "Sets the command and environment variables for the DockerOperator"
        generated_cmd = self.build_cmd()
        env = self.get_env(context=context)

        # set the instance params that the DockerOperator wil pick up
        self.command = generated_cmd
        self.environment = env

        logger.info("Passing the following command to Docker: `%s`", generated_cmd)

    def execute(self, context: Context) -> None:
        "Generates the dbt command and runs the DockerOperator"
        self.prepare(context=context)
        super().execute(context=context)


class DbtLSDockerOperator(DbtDockerBaseOperator):
    "Executes a dbt core ls command."

    ui_color = "#DBCDF6"
    base_cmd = ["ls"]


class DbtSeedDockerOperator(DbtDockerBaseOperator):
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


class DbtSnapshotDockerOperator(DbtDockerBaseOperator):
    "Executes a dbt core snapshot command."

    ui_color = "#964B00"
    base_cmd = ["snapshot"]


class DbtRunDockerOperator(DbtDockerBaseOperator):
    "Executes a dbt core run command."

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"
    base_cmd = ["run"]


class DbtTestDockerOperator(DbtDockerBaseOperator):
    "Executes a dbt core test command."

    ui_color = "#8194E0"
    base_cmd = ["test"]


class DbtRunOperationDockerOperator(DbtDockerBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro. (templated)
    """

    ui_color = "#8194E0"
    template_fields = DbtDockerBaseOperator.template_fields + ["args"]

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
