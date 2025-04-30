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
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

logger = get_logger(__name__)

DEFAULT_ENVIRONMENT_VARIABLES: dict[str, str] = {}

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2

try:
    from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

    # The overrides parameter needed to pass the dbt command was added in apache-airflow-providers-google==10.13.0
    init_signature = inspect.signature(CloudRunExecuteJobOperator.__init__)
    if "overrides" not in init_signature.parameters:
        raise AttributeError(
            "CloudRunExecuteJobOperator does not have `overrides` attribute. Ensure you've installed apache-airflow-providers-google of at least 10.11.0 "
            "separately or with `pip install astronomer-cosmos[...,gcp-cloud-run-job]`."
        )
except ImportError:
    raise ImportError(
        "Could not import CloudRunExecuteJobOperator. Ensure you've installed the Google Cloud provider "
        "separately or with `pip install astronomer-cosmos[...,gcp-cloud-run-job]`."
    )


class DbtGcpCloudRunJobBaseOperator(AbstractDbtBase, CloudRunExecuteJobOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Cloud Run Job instance with dbt installed in it.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(CloudRunExecuteJobOperator.template_fields)
    )

    intercept_flag = False

    def __init__(
        self,
        # arguments required by CloudRunExecuteJobOperator
        project_id: str,
        region: str,
        job_name: str,
        #
        profile_config: ProfileConfig | None = None,
        command: list[str] | None = None,
        environment_variables: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        self.command = command
        self.environment_variables = environment_variables or DEFAULT_ENVIRONMENT_VARIABLES
        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        kwargs.update(
            {
                "project_id": project_id,
                "region": region,
                "job_name": job_name,
            }
        )

        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}
        operator_args: set[str] = set()
        for clazz in CloudRunExecuteJobOperator.__mro__:
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
        CloudRunExecuteJobOperator.__init__(self, **operator_kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = CloudRunExecuteJobOperator.execute(self, context)
        logger.info(result)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.environment_variables = {**env_vars, **self.environment_variables}
        self.command = dbt_cmd
        # Override Cloud Run Job default arguments with dbt command
        self.overrides = {
            "container_overrides": [
                {
                    "args": self.command,
                    "env": [{"name": key, "value": value} for key, value in self.environment_variables.items()],
                }
            ],
        }


class DbtBuildGcpCloudRunJobOperator(DbtBuildMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSGcpCloudRunJobOperator(DbtLSMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedGcpCloudRunJobOperator(DbtSeedMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotGcpCloudRunJobOperator(DbtSnapshotMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceGcpCloudRunJobOperator(DbtSourceMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunGcpCloudRunJobOperator(DbtRunMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestGcpCloudRunJobOperator(DbtTestMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        # as of now, on_warning_callback in docker executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationGcpCloudRunJobOperator(DbtRunOperationMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtGcpCloudRunJobBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneGcpCloudRunJobOperator(DbtCloneMixin, DbtGcpCloudRunJobBaseOperator):
    """
    Executes a dbt core clone command.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
