from __future__ import annotations

import importlib
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosmos.airflow.graph import _snake_case_to_camelcase
from cosmos.config import ProfileConfig
from cosmos.constants import ExecutionMode
from cosmos.dataset import get_dataset_alias_name
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.settings import AIRFLOW_IO_AVAILABLE, remote_target_path, remote_target_path_conn_id

log = logging.getLogger(__name__)


def _create_async_operator_class(profile_type: str, dbt_class: str) -> Any:
    """
    Dynamically constructs and returns an asynchronous operator class for the given profile type and dbt class name.

    The function constructs a class path string for an asynchronous operator, based on the provided `profile_type` and
    `dbt_class`. It attempts to import the corresponding class dynamically and return it. If the class cannot be found,
    it raises an error.

    :param profile_type: The dbt profile type
    :param dbt_class: The dbt class name. Example DbtRun, DbtTest.
    """
    execution_mode = ExecutionMode.AIRFLOW_ASYNC.value
    class_path = f"cosmos.operators._asynchronous.{profile_type}.{dbt_class}{_snake_case_to_camelcase(execution_mode)}{profile_type.capitalize()}Operator"
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        operator_class = getattr(module, class_name)
        return operator_class
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"Error in loading class: {class_path}. Unable to find the specified operator class.") from e


def configure_datasets(operator_kwargs: dict[str, Any], task_id: str) -> dict[str, Any]:
    """
    Sets the outlets for the operator by creating a DatasetAlias.

    :param operator_kwargs: (dict[str, Any])
    :param task_id: (str)
    :return: (dict) Updated operator_kwargs
    """
    from airflow.datasets import DatasetAlias

    # ignoring the type because older versions of Airflow raise the follow error in mypy
    # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")
    dag_id = operator_kwargs.get("dag")
    task_group_id = operator_kwargs.get("task_group")
    operator_kwargs["outlets"] = [
        DatasetAlias(name=get_dataset_alias_name(dag_id, task_group_id, task_id))
    ]  # type: ignore

    return operator_kwargs


def get_remote_sql(async_context: dict[str, Any], project_dir: str) -> str:
    """
    Fetches remote SQL from the remote_target_path.

    :param async_context: (dict[str, Any])
    :param project_dir: (str)
    :return: (str)
    """
    start_time = time.time()

    if not AIRFLOW_IO_AVAILABLE:  # pragma: no cover
        raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")

    from airflow.io.path import ObjectStoragePath

    # file_path: None
    # dbt_dag_task_group_identifier: for DAG/TaskGroup Cosmos builds, pull the ID
    # remote: target_path_str: storage location for compiled SQL
    file_path = async_context["dbt_node_config"]["file_path"]  # type: ignore
    dbt_dag_task_group_identifier = async_context["dbt_dag_task_group_identifier"]
    remote_target_path_str = str(remote_target_path).rstrip("/")

    if TYPE_CHECKING:  # pragma: no cover
        assert project_dir is not None

    # project_dir_parent: None
    # relative_file_path: None
    # remote_model_path: str path to be used when configuring the ObjectStoragePath
    # object_storage_path: ObjectStoragePath where compiled SQL/models are stored
    project_dir_parent = str(Path(project_dir).parent)
    relative_file_path = str(file_path).replace(project_dir_parent, "").lstrip("/")
    remote_model_path = f"{remote_target_path_str}/{dbt_dag_task_group_identifier}/run/{relative_file_path}"
    object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)

    with object_storage_path.open() as fp:  # type: ignore
        sql = fp.read()
        elapsed_time = time.time() - start_time
        log.info("SQL file download completed in %.2f seconds.", elapsed_time)
        return sql  # type: ignore


class DbtRunAirflowAsyncFactoryOperator(DbtRunLocalOperator):  # type: ignore[misc]
    """TODO: Maybe we think about adding the helpers in here?"""

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, object] | None = None,
        dbt_kwargs: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> None:
        self.project_dir = project_dir
        self.profile_config = profile_config

        async_operator_class = self.create_async_operator()

        # Dynamically modify the base classes.
        # This is necessary because the async operator class is only known at runtime.
        # When using composition instead of inheritance to initialize the async class and run its execute method,
        # Airflow throws a `DuplicateTaskIdFound` error.
        DbtRunAirflowAsyncFactoryOperator.__bases__ = (async_operator_class,)
        super().__init__(
            project_dir=project_dir,
            profile_config=profile_config,
            extra_context=extra_context,
            dbt_kwargs=dbt_kwargs,
            **kwargs,
        )

    def create_async_operator(self) -> Any:
        """
        create_async_operator parses the profile_config and loads in the appropriate asynchronous class.

        :return: (Any) The asynchronous operator class
        """
        profile_type = self.profile_config.get_profile_type()
        async_operator_class = _create_async_operator_class(profile_type, "DbtRun")

        return async_operator_class
