from __future__ import annotations

import inspect
from typing import Any, Callable, Sequence

from airflow.utils.context import Context

from cosmos.config import ProfileConfig
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

# ACI is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
except ImportError:
    raise ImportError(
        "Could not import AzureContainerInstancesOperator. Ensure you've installed the Microsoft Azure provider "
        "separately or with `pip install astronomer-cosmos[...,azure-container-instance]`."
    )


class DbtAzureContainerInstanceBaseOperator(AbstractDbtBase, AzureContainerInstancesOperator):  # type: ignore
    """
    Executes a dbt core cli command in an Azure Container Instance
    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(AzureContainerInstancesOperator.template_fields)
    )

    def __init__(
        self,
        ci_conn_id: str,
        resource_group: str,
        name: str,
        image: str,
        region: str,
        profile_config: ProfileConfig | None = None,
        remove_on_error: bool = False,
        fail_if_exists: bool = False,
        registry_conn_id: str | None = None,  # need to add a default for Airflow 2.3 support
        **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        kwargs.update(
            {
                "ci_conn_id": ci_conn_id,
                "resource_group": resource_group,
                "name": name,
                "image": image,
                "region": region,
                "remove_on_error": remove_on_error,
                "fail_if_exists": fail_if_exists,
                "registry_conn_id": registry_conn_id,
            }
        )
        super().__init__(**kwargs)
        base_operator_args = set(inspect.signature(AzureContainerInstancesOperator.__init__).parameters.keys())
        base_kwargs = {}
        for arg_key, arg_value in kwargs.items():
            if arg_key in base_operator_args:
                base_kwargs[arg_key] = arg_value
        base_kwargs["task_id"] = kwargs["task_id"]
        AzureContainerInstancesOperator.__init__(self, **base_kwargs)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = AzureContainerInstancesOperator.execute(self, context)
        self.log.info(result)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.environment_variables: dict[str, Any] = {**env_vars, **self.environment_variables}
        self.command: list[str] = dbt_cmd


class DbtBuildAzureContainerInstanceOperator(DbtBuildMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtAzureContainerInstanceBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSAzureContainerInstanceOperator(DbtLSMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedAzureContainerInstanceOperator(DbtSeedMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtAzureContainerInstanceBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotAzureContainerInstanceOperator(DbtSnapshotMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core snapshot command.

    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceAzureContainerInstanceOperator(DbtSourceMixin, DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunAzureContainerInstanceOperator(DbtRunMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtAzureContainerInstanceBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestAzureContainerInstanceOperator(DbtTestMixin, DbtAzureContainerInstanceBaseOperator):  # type: ignore
    """
    Executes a dbt core test command.
    """

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # as of now, on_warning_callback in azure container instance executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationAzureContainerInstanceOperator(DbtRunOperationMixin, DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = (
        DbtAzureContainerInstanceBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneAzureContainerInstanceOperator(DbtCloneMixin, DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core clone command.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
