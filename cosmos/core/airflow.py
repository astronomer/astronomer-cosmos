import importlib
import logging
from typing import Optional
import inspect

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.core.graph.entities import Task

logger = logging.getLogger(__name__)


def get_airflow_task(task: Task, dag: DAG, task_group: Optional[TaskGroup] = None) -> BaseOperator:
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
    operator = getattr(module, class_name)

    # ensure we only pass the arguments that the operator expects
    supported_args = set()
    for inherited_class in operator.mro():
        for arg in inspect.signature(inherited_class.__init__).parameters:
            supported_args.add(arg)

    potential_operator_args = {
        "task_id": task.id,
        "dag": dag,
        "task_group": task_group,
        **task.arguments,
    }
    operator_args = {
        arg_key: arg_value for arg_key, arg_value in potential_operator_args.items() if arg_key in supported_args
    }

    airflow_task = operator(**operator_args)

    if not isinstance(airflow_task, BaseOperator):
        raise TypeError(f"Operator class {task.operator_class} is not a subclass of BaseOperator")

    return airflow_task
