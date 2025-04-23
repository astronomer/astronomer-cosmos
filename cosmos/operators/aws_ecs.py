from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Callable, Sequence

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.config import ProfileConfig
from cosmos.log import get_logger
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

logger = get_logger(__name__)

DEFAULT_CONN_ID = "aws_default"
DEFAULT_CONTAINER_NAME = "dbt"
DEFAULT_ENVIRONMENT_VARIABLES: dict[str, str] = {}

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2

try:
    from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
except ImportError:  # pragma: no cover
    raise ImportError(
        "Could not import EcsRunTaskOperator. Ensure you've installed the Amazon Web Services provider "
        "separately or with `pip install astronomer-cosmos[...,aws-ecs]`."
    )  # pragma: no cover


class DbtAwsEcsBaseOperator(AbstractDbtBase, EcsRunTaskOperator):  # type: ignore
    """
    Executes a dbt core cli command in an ECS Task instance with dbt installed in it.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(EcsRunTaskOperator.template_fields)
    )

    intercept_flag = False

    def __init__(
        self,
        # arguments required by EcsRunTaskOperator
        cluster: str,
        task_definition: str,
        container_name: str = DEFAULT_CONTAINER_NAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        #
        profile_config: ProfileConfig | None = None,
        command: list[str] | None = None,
        environment_variables: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        self.command = command
        self.environment_variables = environment_variables or DEFAULT_ENVIRONMENT_VARIABLES
        self.container_name = container_name
        kwargs.update(
            {
                "aws_conn_id": aws_conn_id,
                "task_definition": task_definition,
                "cluster": cluster,
                "container_name": container_name,
                "overrides": None,
            }
        )

        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}

        operator_args: set[str] = set()
        for clazz in EcsRunTaskOperator.__mro__:
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
        EcsRunTaskOperator.__init__(self, **operator_kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")

        result = EcsRunTaskOperator.execute(self, context)

        logger.info(result)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.environment_variables = {**env_vars, **self.environment_variables}
        self.command = dbt_cmd
        # Override Ecs Task Run default arguments with dbt command
        self.overrides = {
            "containerOverrides": [
                {
                    "name": self.container_name,
                    "command": self.command,
                    "environment": [{"name": key, "value": value} for key, value in self.environment_variables.items()],
                }
            ]
        }


class DbtBuildAwsEcsOperator(DbtBuildMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtAwsEcsBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSAwsEcsOperator(DbtLSMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedAwsEcsOperator(DbtSeedMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtAwsEcsBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotAwsEcsOperator(DbtSnapshotMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceAwsEcsOperator(DbtSourceMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunAwsEcsOperator(DbtRunMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtAwsEcsBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestAwsEcsOperator(DbtTestMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        # as of now, on_warning_callback in docker executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationAwsEcsOperator(DbtRunOperationMixin, DbtAwsEcsBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtAwsEcsBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
