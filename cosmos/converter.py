# mypy: ignore-errors
# ignoring enum Mypy errors

from __future__ import annotations

import copy
import inspect
import os
import platform
import time
from typing import Any, Callable
from warnings import warn

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos import cache, settings
from cosmos.airflow.graph import build_airflow_graph
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.dbt.graph import DbtGraph
from cosmos.dbt.project import has_non_empty_dependencies_file
from cosmos.dbt.selector import retrieve_by_label
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger

logger = get_logger(__name__)


def migrate_to_new_interface(
    execution_config: ExecutionConfig, project_config: ProjectConfig, render_config: RenderConfig
):
    # We copy the configuration so the change does not affect other DAGs or TaskGroups
    # that may reuse the same original configuration
    render_config = copy.deepcopy(render_config)
    execution_config = copy.deepcopy(execution_config)
    render_config.project_path = project_config.dbt_project_path
    execution_config.project_path = project_config.dbt_project_path
    return execution_config, render_config


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
        if arg_key not in non_airflow_kwargs or arg_key == "dag":
            new_kwargs[arg_key] = arg_value
    return new_kwargs


def validate_arguments(
    render_config: RenderConfig,
    profile_config: ProfileConfig,
    task_args: dict[str, Any],
    execution_config: ExecutionConfig,
    project_config: ProjectConfig,
) -> None:
    """
    Validate that mutually exclusive selectors filters have not been given.
    Validate deprecated arguments.

    :param select: A list of dbt select arguments (e.g. 'config.materialized:incremental')
    :param exclude: A list of dbt exclude arguments (e.g. 'tag:nightly')
    :param profile_config: ProfileConfig Object
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param execution_mode: the current execution mode
    """
    for field in ("tags", "paths"):
        select_items = retrieve_by_label(render_config.select, field)
        exclude_items = retrieve_by_label(render_config.exclude, field)
        intersection = {str(item) for item in set(select_items).intersection(exclude_items)}
        if intersection:
            raise CosmosValueError(f"Can't specify the same {field[:-1]} in `select` and `exclude`: " f"{intersection}")

    # if task_args has a schema, add it to the profile args and add a deprecated warning
    if "schema" in task_args:
        logger.warning("Specifying a schema in the `task_args` is deprecated. Please use the `profile_args` instead.")
        if profile_config.profile_mapping:
            profile_config.profile_mapping.profile_args["schema"] = task_args["schema"]

    if execution_config.execution_mode in [ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV]:
        profile_config.validate_profiles_yml()
        has_non_empty_dependencies = execution_config.project_path and has_non_empty_dependencies_file(
            execution_config.project_path
        )
        if (
            has_non_empty_dependencies
            and (
                render_config.load_method == LoadMode.DBT_LS
                or (render_config.load_method == LoadMode.AUTOMATIC and not project_config.is_manifest_available())
            )
            and (render_config.dbt_deps != task_args.get("install_deps", True))
        ):
            err_msg = f"When using `LoadMode.DBT_LS` and `{execution_config.execution_mode}`, the value of `dbt_deps` in `RenderConfig` should be the same as the `operator_args['install_deps']` value."
            raise CosmosValueError(err_msg)


def validate_initial_user_config(
    execution_config: ExecutionConfig,
    profile_config: ProfileConfig | None,
    project_config: ProjectConfig,
    render_config: RenderConfig,
    operator_args: dict[str, Any],
):
    """
    Validates if the user set the fields as expected.

    :param execution_config: Configuration related to how to run dbt in Airflow tasks
    :param profile_config: Configuration related to dbt database configuration (profile)
    :param project_config: Configuration related to the overall dbt project
    :param render_config: Configuration related to how to convert the dbt workflow into an Airflow DAG
    :param operator_args: Arguments to pass to the underlying operators.
    """
    if profile_config is None and execution_config.execution_mode in (
        ExecutionMode.LOCAL,
        ExecutionMode.VIRTUALENV,
    ):
        raise CosmosValueError(f"The profile_config is mandatory when using {execution_config.execution_mode}")

    # Since we now support both project_config.dbt_project_path, render_config.project_path and execution_config.project_path
    # We need to ensure that only one interface is being used.
    if project_config.dbt_project_path and (render_config.project_path or execution_config.project_path):
        raise CosmosValueError(
            "ProjectConfig.dbt_project_path is mutually exclusive with RenderConfig.dbt_project_path and ExecutionConfig.dbt_project_path."
            + "If using RenderConfig.dbt_project_path or ExecutionConfig.dbt_project_path, ProjectConfig.dbt_project_path should be None"
        )

    # Cosmos 2.0 will remove the ability to pass in operator_args with 'env' and 'vars' in place of ProjectConfig.env_vars and
    # ProjectConfig.dbt_vars.
    if "env" in operator_args:
        warn(
            "operator_args with 'env' is deprecated since Cosmos 1.3 and will be removed in Cosmos 2.0. Use ProjectConfig.env_vars instead.",
            DeprecationWarning,
        )
        if project_config.env_vars:
            raise CosmosValueError(
                "ProjectConfig.env_vars and operator_args with 'env' are mutually exclusive and only one can be used."
            )
    if "install_deps" in operator_args:
        warn(
            "The operator argument `install_deps` is deprecated since Cosmos 1.9 and will be removed in Cosmos 2.0. Use `ProjectConfig.install_dbt_deps` instead.",
            DeprecationWarning,
        )

    # Cosmos 2.0 will remove the ability to pass RenderConfig.env_vars in place of ProjectConfig.env_vars, check that both are not set.
    if project_config.env_vars and render_config.env_vars:
        raise CosmosValueError(
            "Both ProjectConfig.env_vars and RenderConfig.env_vars were provided. RenderConfig.env_vars is deprecated since Cosmos 1.3, "
            "please use ProjectConfig.env_vars instead."
        )


def validate_changed_config_paths(
    execution_config: ExecutionConfig | None, project_config: ProjectConfig, render_config: RenderConfig | None
):
    """
    Validates if all the necessary fields required by Cosmos to render the DAG are set.

    :param execution_config: Configuration related to how to run dbt in Airflow tasks
    :param project_config: Configuration related to the overall dbt project
    :param render_config: Configuration related to how to convert the dbt workflow into an Airflow DAG
    """
    # At this point, execution_config.project_path should always be non-null
    if not execution_config.project_path:
        raise CosmosValueError(
            "ExecutionConfig.dbt_project_path is required for the execution of dbt tasks in all execution modes."
        )

    # We now have a guaranteed execution_config.project_path, but still need to process render_config.project_path
    # We require render_config.project_path when we dont have a manifest
    if not project_config.manifest_path and not render_config.project_path:
        raise CosmosValueError(
            "RenderConfig.dbt_project_path is required for rendering an airflow DAG from a DBT Graph if no manifest is provided."
        )


def override_configuration(
    project_config: ProjectConfig, render_config: RenderConfig, execution_config: ExecutionConfig, operator_args: dict
) -> None:
    """
    There are a few scenarios where a configuration should override another one.
    This function changes, in place, render_config, execution_config and operator_args depending on other configurations.
    """
    if project_config.dbt_project_path:
        render_config.project_path = project_config.dbt_project_path
        execution_config.project_path = project_config.dbt_project_path

    if render_config.dbt_deps is None:
        render_config.dbt_deps = project_config.install_dbt_deps

    if execution_config.dbt_executable_path:
        operator_args["dbt_executable_path"] = execution_config.dbt_executable_path

    if execution_config.invocation_mode:
        operator_args["invocation_mode"] = execution_config.invocation_mode

    if execution_config.execution_mode in (ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV):
        if "install_deps" not in operator_args:
            operator_args["install_deps"] = project_config.install_dbt_deps
        if "copy_dbt_packages" not in operator_args:
            operator_args["copy_dbt_packages"] = project_config.copy_dbt_packages


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
        profile_config: ProfileConfig | None = None,
        execution_config: ExecutionConfig | None = None,
        render_config: RenderConfig | None = None,
        dag: DAG | None = None,
        task_group: TaskGroup | None = None,
        operator_args: dict[str, Any] | None = None,
        on_warning_callback: Callable[..., Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:

        # We copy the configuration so the changes introduced in this method, such as override_configuration,
        # do not affect other DAGs or TaskGroups that may reuse the same original configuration
        execution_config = copy.deepcopy(execution_config) if execution_config is not None else ExecutionConfig()
        render_config = copy.deepcopy(render_config) if render_config is not None else RenderConfig()
        operator_args = copy.deepcopy(operator_args) if operator_args is not None else {}

        project_config.validate_project()
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)
        override_configuration(project_config, render_config, execution_config, operator_args)
        validate_changed_config_paths(execution_config, project_config, render_config)

        if execution_config.execution_mode != ExecutionMode.VIRTUALENV and execution_config.virtualenv_dir is not None:
            logger.warning(
                "`ExecutionConfig.virtualenv_dir` is only supported when \
                ExecutionConfig.execution_mode is set to ExecutionMode.VIRTUALENV."
            )

        cache_dir = None
        cache_identifier = None
        if settings.enable_cache:
            cache_identifier = cache._create_cache_identifier(dag, task_group)
            cache_dir = cache._obtain_cache_dir_path(cache_identifier=cache_identifier)

        previous_time = time.perf_counter()
        self.dbt_graph = DbtGraph(
            project=project_config,
            render_config=render_config,
            execution_config=execution_config,
            profile_config=profile_config,
            cache_dir=cache_dir,
            cache_identifier=cache_identifier,
            dbt_vars=project_config.dbt_vars,
            airflow_metadata=cache._get_airflow_metadata(dag, task_group),
        )
        self.dbt_graph.load(method=render_config.load_method, execution_mode=execution_config.execution_mode)

        current_time = time.perf_counter()
        elapsed_time = current_time - previous_time
        logger.info(
            f"Cosmos performance ({cache_identifier}) -  [{platform.node()}|{os.getpid()}]: It took {elapsed_time:.3}s to parse the dbt project for DAG using {self.dbt_graph.load_method}"
        )
        previous_time = current_time

        env_vars = operator_args.get("env") or project_config.env_vars
        dbt_vars = operator_args.get("vars") or project_config.dbt_vars
        task_args = {
            **operator_args,
            "project_dir": execution_config.project_path,
            "partial_parse": project_config.partial_parse,
            "profile_config": profile_config,
            "emit_datasets": render_config.emit_datasets,
            "env": env_vars,
            "vars": dbt_vars,
            "cache_dir": cache_dir,
        }

        validate_arguments(
            execution_config=execution_config,
            profile_config=profile_config,
            render_config=render_config,
            task_args=task_args,
            project_config=project_config,
        )

        if execution_config.execution_mode == ExecutionMode.VIRTUALENV and execution_config.virtualenv_dir is not None:
            task_args["virtualenv_dir"] = execution_config.virtualenv_dir

        self.tasks_map = build_airflow_graph(
            nodes=self.dbt_graph.filtered_nodes,
            dag=dag or (task_group and task_group.dag),
            task_group=task_group,
            execution_mode=execution_config.execution_mode,
            task_args=task_args,
            test_indirect_selection=execution_config.test_indirect_selection,
            dbt_project_name=render_config.project_name,
            on_warning_callback=on_warning_callback,
            render_config=render_config,
            async_py_requirements=execution_config.async_py_requirements,
        )

        current_time = time.perf_counter()
        elapsed_time = current_time - previous_time
        logger.info(
            f"Cosmos performance ({cache_identifier}) - [{platform.node()}|{os.getpid()}]: It took {elapsed_time:.3}s to build the Airflow DAG."
        )
