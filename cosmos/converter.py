"Contains the DbtToAirflowConverter class which is used to convert a dbt project into an Airflow DAG."

from __future__ import annotations

import inspect
import logging
from typing import Any

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from pathlib import Path

from cosmos.constants import ExecutionMode
from cosmos.airflow.graph import build_airflow_graph
from cosmos.dbt.graph import DbtGraph
from cosmos.config import ProjectConfig, RenderConfig, ProfileConfig, ExecutionConfig, CosmosConfig
from cosmos.exceptions import CosmosValueError


logger = logging.getLogger(__name__)


def specific_kwargs(**kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Extract kwargs specific to the cosmos.converter.DbtToAirflowConverter class initialization method.

    :param kwargs: kwargs which can contain DbtToAirflowConverter and non DbtToAirflowConverter kwargs.
    """
    new_kwargs = {}
    specific_args_keys = inspect.getfullargspec(DbtToAirflowConverter.__init__).args
    for arg_key, arg_value in kwargs.items():
        if arg_key in specific_args_keys:
            new_kwargs[arg_key] = arg_value

    return new_kwargs


def airflow_kwargs(**kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Extract kwargs specific to the Airflow DAG or TaskGroup class initialization method.

    :param kwargs: kwargs which can contain Airflow DAG or TaskGroup and cosmos.converter.DbtToAirflowConverter kwargs.
    """
    new_kwargs = {}
    non_airflow_kwargs = specific_kwargs(**kwargs)
    for arg_key, arg_value in kwargs.items():
        if arg_key not in non_airflow_kwargs:
            new_kwargs[arg_key] = arg_value

    return new_kwargs


def validate_configs(
    project_config: ProjectConfig,
    render_config: RenderConfig,
    execution_config: ExecutionConfig,
    profile_config: ProfileConfig,
) -> None:
    "Validates all configuration to ensure that they are compatible with each other."
    project_config.validate_project()

    # if we're in local or venv mode, make sure we have a profile
    if execution_config.execution_mode in [ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV] and not profile_config:
        if not profile_config:
            raise CosmosValueError("You must provide a profile_config when using local or venv execution mode")

        profile_config.validate_profile()


class DbtToAirflowConverter:
    """
    Logic common to build an Airflow DbtDag and DbtTaskGroup from a DBT project.

    :param dag: Airflow DAG to be populated
    :param project_config: A ProjectConfig object to point Cosmos at a dbt project.
    :param profile_config: A ProfileConfig object to use to render and execute dbt.
    :param render_config: A RenderConfig object to use to render the dbt project.
    :param execution_config: An ExecutionConfig object to use to execute the dbt project.
    :param operator_args: Parameters to pass to the underlying operators. See the documentation for
        the specific operator for more information.
    :param task_group (optional): Airflow Task Group to be populated
    """

    def __init__(
        self,
        project_config: ProjectConfig,
        profile_config: ProfileConfig,
        render_config: RenderConfig = RenderConfig(),
        execution_config: ExecutionConfig = ExecutionConfig(),
        operator_args: dict[str, Any] | None = None,
        dag: DAG | None = None,
        task_group: TaskGroup | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        cosmos_config = CosmosConfig(
            project_config=project_config,
            profile_config=profile_config,
            render_config=render_config,
            execution_config=execution_config,
        )

        if not operator_args:
            operator_args = {}

        validate_configs(
            project_config=project_config,
            render_config=render_config,
            execution_config=execution_config,
            profile_config=profile_config,
        )

        dbt_graph = DbtGraph(cosmos_config)
        dbt_graph.load()

        task_args = {
            "cosmos_config": cosmos_config,
            **operator_args,
        }

        if task_group and not dag:
            if not task_group.dag:
                raise CosmosValueError(
                    "task_group must be associated with a DAG or you must pass a DAG to the constructor"
                )

            dag = task_group.dag

        if not dag:
            raise CosmosValueError("You must pass a DAG to the constructor")

        build_airflow_graph(
            nodes=dbt_graph.nodes,
            dag=dag,
            task_group=task_group,
            cosmos_config=cosmos_config,
            task_args=task_args,
        )
