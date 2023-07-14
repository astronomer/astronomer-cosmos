from __future__ import annotations

import inspect
import logging
import pathlib
from typing import Any, Callable, Optional

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.airflow.graph import build_airflow_graph
from cosmos.dbt.executable import get_system_dbt
from cosmos.dbt.graph import DbtGraph, LoadMode
from cosmos.dbt.project import DbtProject


logger = logging.getLogger(__name__)


def specific_kwargs(**kwargs):
    """
    Extract kwargs specific to the cosmos.airflow.AirflowGroup class initialization method.
    """
    new_kwargs = {}
    specific_args_keys = inspect.getfullargspec(DbtToAirflowConverter.__init__).args
    for arg_key, arg_value in kwargs.items():
        if arg_key in specific_args_keys:
            new_kwargs[arg_key] = arg_value
    return new_kwargs


def airflow_kwargs(**kwargs):
    """
    Extract kwargs specific to the Airflow DAG or TaskGroup class initialization method.
    """
    new_kwargs = {}
    non_airflow_kwargs = specific_kwargs(**kwargs)
    for arg_key, arg_value in kwargs.items():
        if arg_key not in non_airflow_kwargs:
            new_kwargs[arg_key] = arg_value
    return new_kwargs


def validate_arguments(select, exclude, profile_args, task_args) -> None:
    """
    Validate that selectors mutually exclusive filters have not been given.
    Validate deprecated arguments.
    """

    if "tags" in select and "tags" in exclude:
        if set(select["tags"]).intersection(exclude["tags"]):
            raise AirflowException(
                f"Can't specify the same tag in `select` and `include`: "
                f"{set(select['tags']).intersection(exclude['tags'])}"
            )

    if "paths" in select and "paths" in exclude:
        if set(select["paths"]).intersection(exclude["paths"]):
            raise AirflowException(
                f"Can't specify the same path in `select` and `include`: "
                f"{set(select['paths']).intersection(exclude['paths'])}"
            )

    # if task_args has a schema, add it to the profile args and add a deprecated warning
    if "schema" in task_args:
        profile_args["schema"] = task_args["schema"]
        logger.warning("Specifying a schema in the task_args is deprecated. Please use the profile_args instead.")


class DbtToAirflowConverter:
    """
    Logic common to build an Airflow DbtDag and DbtTaskGroup from a DBT project.

    :param dag: Airflow DAG to be populated
    :param task_group (optional): Airflow Task Group to be populated
    :param dbt_project_name: The name of the dbt project
    :param dbt_root_path: The path to the dbt root directory
    :param dbt_models_dir: The path to the dbt models directory within the project
    :param dbt_seeds_dir: The path to the dbt seeds directory within the project
    :param conn_id: The Airflow connection ID to use for the dbt profile
    :param profile_args: Arguments to pass to the dbt profile
    :param profile_name_override: A name to use for the dbt profile. If not provided, and no profile target is found
        in your project's dbt_project.yml, "cosmos_profile" is used.
    :param target_name_override: A name to use for the dbt target. If not provided, "cosmos_target" is used.
    :param dbt_args: Parameters to pass to the underlying dbt operators, can include dbt_executable_path to utilize venv
    :param operator_args: Parameters to pass to the underlying operators, can include KubernetesPodOperator
        or DockerOperator parameters
    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
    :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
        Defaults to "after_each"
    :param select: A list of dbt select arguments (e.g. 'config.materialized:incremental')
    :param exclude: A list of dbt exclude arguments (e.g. 'tag:nightly')
    :param execution_mode: The execution mode in which the dbt project should be run.
        Options are "local", "virtualenv", "docker", and "kubernetes".
        Defaults to "local"
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    def __init__(
        self,
        dbt_project_name: str,
        conn_id: str,
        dag: DAG | None = None,
        task_group: TaskGroup | None = None,
        profile_args: dict[str, str] = {},
        dbt_args: dict[str, Any] = {},
        profile_name_override: str | None = None,
        target_name_override: str | None = None,
        operator_args: dict[str, Any] = {},
        emit_datasets: bool = True,
        dbt_root_path: str = "/usr/local/airflow/dags/dbt",
        dbt_models_dir: str | None = None,
        dbt_seeds_dir: str | None = None,
        dbt_snapshots_dir: str | None = None,
        test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
        select: list[str] | None = None,
        exclude: list[str] | None = None,
        execution_mode: Literal["local", "docker", "kubernetes", "virtualenv"] = "local",
        load_mode: LoadMode = LoadMode.AUTOMATIC,
        manifest_path: str | pathlib.Path | None = None,
        on_warning_callback: Optional[Callable] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        select = select or []
        exclude = exclude or []

        dbt_project = DbtProject(
            name=dbt_project_name,
            root_dir=dbt_root_path,
            models_dir=dbt_models_dir,
            seeds_dir=dbt_seeds_dir,
            snapshots_dir=dbt_snapshots_dir,
            manifest_path=manifest_path,
        )

        dbt_graph = DbtGraph(
            project=dbt_project,
            exclude=exclude,
            select=select,
            dbt_cmd=dbt_args.get("dbt_executable_path", get_system_dbt()),
        )
        dbt_graph.load(method=load_mode, execution_mode=execution_mode)

        task_args = {
            **dbt_args,
            **operator_args,
            "profile_args": profile_args,
            "profile_name": profile_name_override,
            "target_name": target_name_override,
            # the following args may be only needed for local / venv:
            "project_dir": dbt_project.dir,
            "conn_id": conn_id,
        }

        validate_arguments(select, exclude, profile_args, task_args)

        build_airflow_graph(
            nodes=dbt_graph.nodes,
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
