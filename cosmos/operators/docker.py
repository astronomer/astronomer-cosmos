from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Callable, Sequence

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2
if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.config import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

# docker is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    raise ImportError(
        "Could not import DockerOperator. Ensure you've installed the docker provider separately or "
        "with with `pip install astronomer-cosmos[...,docker]`."
    )


class DbtDockerBaseOperator(AbstractDbtBase, DockerOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Docker container.

    """

    template_fields: Sequence[str] = tuple(list(AbstractDbtBase.template_fields) + list(DockerOperator.template_fields))

    intercept_flag = False

    def __init__(
        self,
        image: str,  # Make image a required argument since it's required by DockerOperator
        profile_config: ProfileConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        if self.profile_config and not self.profile_config.profiles_yml_filepath:
            raise CosmosValueError(
                "For ExecutionMode.DOCKER, specifying ProfileConfig only works with profiles_yml_filepath method and "
                "it must be specified. ProfileConfig with ProfileMapping method is not supported as the underlying "
                "Airflow connections are not available in the Docker container for the mapping to work."
            )

        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        kwargs["image"] = image

        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}
        operator_args: set[str] = set()
        for clazz in DockerOperator.__mro__:
            operator_args.update(inspect.signature(clazz.__init__).parameters.keys())
            if clazz == BaseOperator:
                break
        for arg in operator_args:
            try:
                operator_kwargs[arg] = kwargs[arg]
            except KeyError:
                pass

        base_kwargs = {}
        for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
            try:
                base_kwargs[arg] = kwargs[arg]
            except KeyError:
                try:
                    base_kwargs[arg] = default_args[arg]
                except KeyError:
                    pass
        AbstractDbtBase.__init__(self, **base_kwargs)
        DockerOperator.__init__(self, **operator_kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = DockerOperator.execute(self, context)
        self.log.info(result)

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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSDockerOperator(DbtLSMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedDockerOperator(DbtSeedMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotDockerOperator(DbtSnapshotMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceDockerOperator(DbtSourceMixin, DbtDockerBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunDockerOperator(DbtRunMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtDockerBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneDockerOperator(DbtCloneMixin, DbtDockerBaseOperator):
    """
    Executes a dbt core clone command.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
