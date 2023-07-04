"""
This module contains a function to render a dbt project as an Airflow DAG.
"""
from __future__ import annotations

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from typing import Any, Callable, Dict, List, Optional

from airflow.models.dag import DAG


class DbtDag(DAG):
    """
    Render a dbt project as an Airflow DAG. Overrides the Airflow DAG model to allow
    for additional configs to be passed.

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
    :param select: A dict of dbt selector arguments (i.e., {"tags": ["tag_1", "tag_2"]})
    :param exclude: A dict of dbt exclude arguments (i.e., {"tags": ["tag_1", "tag_2"]})
    :param execution_mode: The execution mode in which the dbt project should be run.
        Options are "local", "virtualenv", "docker", and "kubernetes".
        Defaults to "local"
    :param on_warning_callback: A callback function called on warnings with additional Context variables "test_names"
        and "test_results" of type `List`. Each index in "test_names" corresponds to the same index in "test_results".
    """

    # _task_group = None

    def __init__(
        self,
        dbt_project_name: str,
        conn_id: str,
        profile_args: Dict[str, str] = {},
        dbt_args: Dict[str, Any] = {},
        profile_name_override: str | None = None,
        target_name_override: str | None = None,
        operator_args: Dict[str, Any] = {},
        emit_datasets: bool = True,
        dbt_root_path: str = "/usr/local/airflow/dags/dbt",
        dbt_models_dir: str = "models",
        dbt_seeds_dir: str = "seeds",
        test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
        select: Dict[str, List[str]] = {},
        exclude: Dict[str, List[str]] = {},
        execution_mode: Literal["local", "docker", "kubernetes", "virtualenv"] = "local",
        on_warning_callback: Optional[Callable] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # add additional args to the dbt_args
        dbt_args = {
            **dbt_args,
            "conn_id": conn_id,
        }
        super().__init__(*args, **kwargs)

        from cosmos.builder import extract_dbt_nodes, add_airflow_entities

        nodes = extract_dbt_nodes(
            project_dir=dbt_root_path / dbt_project_name,
            # project_dir="/tmp/dbt_project",
            resource_type=None,
            select=None,
            models=None,
            exclude=None,
            selector=None,
        )

        # filter out nodes as needed

        add_airflow_entities(
            nodes=nodes,
            dag=self,
            execution_mode=execution_mode,
            project_dir=dbt_root_path / dbt_project_name,
            conn_id=conn_id,
            profile_args=profile_args,
        )

        # get the group of the dbt project
        """
        group = render_project(
            dbt_project_name=dbt_project_name,
            dbt_root_path=dbt_root_path,
            dbt_models_dir=dbt_models_dir,
            dbt_seeds_dir=dbt_seeds_dir,
            task_args=dbt_args,
            operator_args=operator_args,
            test_behavior=test_behavior,
            emit_datasets=emit_datasets,
            conn_id=conn_id,
            profile_args=profile_args,
            profile_name=profile_name_override,
            target_name=target_name_override,
            select=select,
            exclude=exclude,
            execution_mode=execution_mode,
            on_warning_callback=on_warning_callback,
        )
        """

        # call the airflow DAG constructor

        # super().__init__(group, *args, **kwargs)
