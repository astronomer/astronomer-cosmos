from __future__ import annotations

from typing import Any, Callable, Sequence

import yaml
from airflow.utils.context import Context
from cosmos.config import ProfileConfig

from cosmos.log import get_logger
from cosmos.operators.base import DbtBaseOperator

logger = get_logger(__name__)

# ACI is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
except ImportError:
    raise ImportError(
        "Could not import AzureContainerInstancesOperator. Ensure you've installed the Microsoft Azure provider "
        "separately or with `pip install astronomer-cosmos[...,azure-container-instance]`."
    )


class DbtAzureContainerInstanceBaseOperator(AzureContainerInstancesOperator, DbtBaseOperator):  # type: ignore
    """
    Executes a dbt core cli command in an Azure Container Instance
    """

    template_fields: Sequence[str] = tuple(
        list(DbtBaseOperator.template_fields) + list(AzureContainerInstancesOperator.template_fields)
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
        super().__init__(
            ci_conn_id=ci_conn_id,
            resource_group=resource_group,
            name=name,
            image=image,
            region=region,
            remove_on_error=remove_on_error,
            fail_if_exists=fail_if_exists,
            registry_conn_id=registry_conn_id,
            **kwargs,
        )

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> int:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = int(super().execute(context))
        logger.info(result)
        return result

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        self.environment_variables: dict[str, Any] = {**env_vars, **self.environment_variables}
        self.command: list[str] = dbt_cmd

    def execute(self, context: Context) -> int:
        return self.build_and_run_cmd(context=context)


class DbtLSAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["ls"]


class DbtSeedAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"

    def __init__(self, full_refresh: bool = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = ["seed"]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context) -> int:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtSnapshotAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core snapshot command.

    """

    ui_color = "#964B00"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["snapshot"]


class DbtRunAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["run"]


class DbtTestAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core test command.
    """

    ui_color = "#8194E0"

    def __init__(self, on_warning_callback: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["test"]
        # as of now, on_warning_callback in azure container instance executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = ("args",)

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
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

    def execute(self, context: Context) -> int:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
