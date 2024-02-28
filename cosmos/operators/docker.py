from __future__ import annotations

from typing import Any, Callable, Sequence

from airflow.utils.context import Context

from cosmos.log import get_logger
from cosmos.operators.base import (
    AbstractDbtBaseOperator,
    DbtBuildMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtTestMixin,
)

logger = get_logger(__name__)

# docker is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    raise ImportError(
        "Could not import DockerOperator. Ensure you've installed the docker provider separately or "
        "with with `pip install astronomer-cosmos[...,docker]`."
    )


class DbtDockerBaseOperator(AbstractDbtBaseOperator, DockerOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Docker container.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBaseOperator.template_fields) + list(DockerOperator.template_fields)
    )

    intercept_flag = False

    def __init__(
        self,
        image: str,  # Make image a required argument since it's required by DockerOperator
        **kwargs: Any,
    ) -> None:
        super().__init__(image=image, **kwargs)

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = DockerOperator.execute(self, context)
        logger.info(result)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        # set env vars
        self.environment: dict[str, Any] = {**env_vars, **self.environment}
        self.command: list[str] = dbt_cmd


class DbtBuildDockerOperator(DbtBuildMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]


class DbtLSDockerOperator(DbtLSMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core ls command.
    """


class DbtSeedDockerOperator(DbtSeedMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]


class DbtSnapshotDockerOperator(DbtSnapshotMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core snapshot command.
    """


class DbtRunDockerOperator(DbtRunMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]


class DbtTestDockerOperator(DbtTestMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        # as of now, on_warning_callback in docker executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationDockerOperator(DbtRunOperationMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]
