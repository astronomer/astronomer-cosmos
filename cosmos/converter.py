# mypy: ignore-errors
# ignoring enum Mypy errors

from __future__ import annotations

import inspect
from typing import Any, Callable

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.airflow.graph import build_airflow_graph
from cosmos.dbt.graph import DbtGraph
from cosmos.dbt.selector import retrieve_by_label
from cosmos.config import ProjectConfig, ExecutionConfig, RenderConfig, ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger


logger = get_logger(__name__)


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
        test_behavior = render_config.test_behavior
        select = render_config.select
        exclude = render_config.exclude
        dbt_deps = render_config.dbt_deps
        execution_mode = execution_config.execution_mode
        test_indirect_selection = execution_config.test_indirect_selection
        load_mode = render_config.load_method
        dbt_executable_path = execution_config.dbt_executable_path
        node_converters = render_config.node_converters

        if not project_config.dbt_project_path:
            raise CosmosValueError("A Project Path in ProjectConfig is required for generating a Task Operators.")

        profile_args = {}
        if profile_config.profile_mapping:
            profile_args = profile_config.profile_mapping.profile_args

        if not operator_args:
            operator_args = {}

        # Previously, we were creating a cosmos.dbt.project.DbtProject
        # DbtProject has now been replaced with ProjectConfig directly
        #   since the interface of the two classes were effectively the same
        # Under this previous implementation, we were passing:
        #  - name, root dir, models dir, snapshots dir and manifest path
        # Internally in the dbtProject class, we were defaulting the profile_path
        #   To be root dir/profiles.yml
        # To keep this logic working, if converter is given no ProfileConfig,
        #   we can create a default retaining this value to preserve this functionality.
        # We may want to consider defaulting this value in our actual ProjceConfig class?
        dbt_graph = DbtGraph(
            project=project_config,
            exclude=exclude,
            select=select,
            dbt_cmd=dbt_executable_path,
            profile_config=profile_config,
            operator_args=operator_args,
            dbt_deps=dbt_deps,
        )
        dbt_graph.load(method=load_mode, execution_mode=execution_mode)

        task_args = {
            **operator_args,
            # the following args may be only needed for local / venv:
            "project_dir": project_config.dbt_project_path,
            "profile_config": profile_config,
            "emit_datasets": emit_datasets,
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
            test_indirect_selection=test_indirect_selection,
            dbt_project_name=project_config.project_name,
            on_warning_callback=on_warning_callback,
            node_converters=node_converters,
        )
