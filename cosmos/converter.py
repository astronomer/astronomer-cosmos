# mypy: ignore-errors
# ignoring enum Mypy errors

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable
from pathlib import Path

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.airflow.graph import build_airflow_graph
from cosmos.dbt.graph import DbtGraph
from cosmos.dbt.project import DbtProject
from cosmos.dbt.selector import retrieve_by_label
from cosmos.config import ProjectConfig, ExecutionConfig, RenderConfig, ProfileConfig
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


def validate_arguments(
    select: list[str], exclude: list[str], profile_args: dict[str, Any], task_args: dict[str, Any]
) -> None:
    """
    Validate that mutually exclusive selectors filters have not been given.
    Validate deprecated arguments.

    :param select: A list of dbt select arguments (e.g. 'config.materialized:incremental')
    :param exclude: A list of dbt exclude arguments (e.g. 'tag:nightly')
    :param profile_args: Arguments to pass to the dbt profile
    :param task_args: Arguments to be used to instantiate an Airflow Task
    """
    for field in ("tags", "paths"):
        select_items = retrieve_by_label(select, field)
        exclude_items = retrieve_by_label(exclude, field)
        intersection = {str(item) for item in set(select_items).intersection(exclude_items)}
        if intersection:
            raise CosmosValueError(f"Can't specify the same {field[:-1]} in `select` and `exclude`: " f"{intersection}")

    # if task_args has a schema, add it to the profile args and add a deprecated warning
    if "schema" in task_args:
        profile_args["schema"] = task_args["schema"]
        logger.warning("Specifying a schema in the `task_args` is deprecated. Please use the `profile_args` instead.")


class DbtToAirflowConverter:
    """
    Logic common to build an Airflow DbtDag and DbtTaskGroup from a DBT project.

    :param dag: Airflow DAG to be populated
    :param task_group (optional): Airflow Task Group to be populated
    :param project_config: The dbt project configuration
    :param execution_config: The dbt execution configuration
    :param render_config: The dbt render configuration
    :param operator_args: Parameters to pass to the underlying operators, can include KubernetesPodOperator
        or DockerOperator parameters
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    def __init__(
        self,
        project_config: ProjectConfig,
        profile_config: ProfileConfig,
        execution_config: ExecutionConfig = ExecutionConfig(),
        render_config: RenderConfig = RenderConfig(),
        dag: DAG | None = None,
        task_group: TaskGroup | None = None,
        operator_args: dict[str, Any] | None = None,
        on_warning_callback: Callable[..., Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        project_config.validate_project()

        emit_datasets = render_config.emit_datasets
        dbt_root_path = project_config.dbt_project_path.parent
        dbt_project_name = project_config.dbt_project_path.name
        dbt_models_dir = project_config.models_relative_path
        dbt_seeds_dir = project_config.seeds_relative_path
        dbt_snapshots_dir = project_config.snapshots_relative_path
        test_behavior = render_config.test_behavior
        select = render_config.select
        exclude = render_config.exclude
        execution_mode = execution_config.execution_mode
        load_mode = render_config.load_method
        manifest_path = project_config.parsed_manifest_path
        dbt_executable_path = execution_config.dbt_executable_path

        conn_id = "unknown"
        if profile_config and profile_config.profile_mapping:
            conn_id = profile_config.profile_mapping.conn_id

        profile_args = {}
        if profile_config.profile_mapping:
            profile_args = profile_config.profile_mapping.profile_args

        if not operator_args:
            operator_args = {}

        dbt_project = DbtProject(
            name=dbt_project_name,
            root_dir=Path(dbt_root_path),
            models_dir=Path(dbt_models_dir) if dbt_models_dir else None,
            seeds_dir=Path(dbt_seeds_dir) if dbt_seeds_dir else None,
            snapshots_dir=Path(dbt_snapshots_dir) if dbt_snapshots_dir else None,
            manifest_path=manifest_path,
        )

        dbt_graph = DbtGraph(
            project=dbt_project,
            exclude=exclude,
            select=select,
            dbt_cmd=dbt_executable_path,
            profile_config=profile_config,
        )
        dbt_graph.load(method=load_mode, execution_mode=execution_mode)

        task_args = {
            **operator_args,
            # the following args may be only needed for local / venv:
            "project_dir": dbt_project.dir,
            "profile_config": profile_config,
        }

        if dbt_executable_path:
            task_args["dbt_executable_path"] = dbt_executable_path

        validate_arguments(select, exclude, profile_args, task_args)

        build_airflow_graph(
            nodes=dbt_graph.filtered_nodes,
            dag=dag or (task_group and task_group.dag),
            task_group=task_group,
            execution_mode=execution_mode,
            task_args=task_args,
            test_behavior=test_behavior,
            dbt_project_name=dbt_project.name,
            conn_id=conn_id,
            on_warning_callback=on_warning_callback,
            emit_datasets=emit_datasets,
        )
