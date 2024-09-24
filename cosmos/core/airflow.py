from __future__ import annotations

import importlib

import airflow
from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from packaging.version import Version

from cosmos.core.graph.entities import Task
from cosmos.dataset import get_dataset_alias_name
from cosmos.log import get_logger

logger = get_logger(__name__)
AIRFLOW_VERSION = Version(airflow.__version__)


def get_airflow_task(task: Task, dag: DAG, task_group: TaskGroup | None = None) -> BaseOperator:
    """
    Get the Airflow Operator class for a Task.

    :param task: The Task to get the Operator for

    :return: The Operator class
    :rtype: BaseOperator
    """
    # first, import the operator class from the
    # fully qualified name defined in the task
    module_name, class_name = task.operator_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    Operator = getattr(module, class_name)

    task_kwargs = {}
    if task.owner != "":
        task_kwargs["owner"] = task.owner

    if module_name.split(".")[-1] == "local" and AIRFLOW_VERSION >= Version("2.10"):
        from airflow.datasets import DatasetAlias

        # ignoring the type because older versions of Airflow raise the follow error in mypy
        # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")  [assignment] Found 1 error in 1 file (checked 3 source files)
        task_kwargs["outlets"] = [DatasetAlias(name=get_dataset_alias_name(dag, task_group, task.id))]  # type: ignore

    logger.info("HELP ME!!!")
    logger.info(module_name)
    logger.info(Operator)
    logger.info(task.id)
    logger.info(dag)
    logger.info(task_group)
    logger.info(task_kwargs)
    logger.info({} if class_name == "EmptyOperator" else {"extra_context": task.extra_context})
    logger.info(task.arguments)

    airflow_task = Operator(
        task_id=task.id,
        dag=dag,
        task_group=task_group,
        **task_kwargs,
        **({} if class_name == "EmptyOperator" else {"extra_context": task.extra_context}),
        **task.arguments,
    )

    if not isinstance(airflow_task, BaseOperator):
        raise TypeError(f"Operator class {task.operator_class} is not a subclass of BaseOperator")

    return airflow_task
