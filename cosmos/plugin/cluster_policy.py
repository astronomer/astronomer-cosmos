from logging import getLogger

from airflow.models.taskinstance import TaskInstance
from airflow.policies import hookimpl
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from cosmos.operators._watcher.base import BaseConsumerSensor
from cosmos.settings import watcher_retry_queue

log = getLogger(__name__)


def _is_watcher_sensor(task_instance: TaskInstance) -> bool:
    """
    Check if the task instance is a watcher sensor.

    In Airflow 3, task_instance.task is a SerializedBaseOperator, isinstance checks won't work.
    Instead, check the task's module path, which is preserved in serialization.
    """
    task = task_instance.task
    # Get the module, using _task_module if available (serialized tasks) or __module__ as fallback
    module = getattr(task, "_task_module", None) or task.__class__.__module__

    # Check if it's from the watcher operators module
    return "cosmos.operators.watcher" in module or isinstance(task, BaseConsumerSensor)


@hookimpl
def task_instance_mutation_hook(task_instance: TaskInstance) -> None:
    # In Airflow 3.x the task_instance_mutation_hook try_number starts at None or 0
    # in Airflow 2.x it starts at 1
    if AIRFLOW_VERSION >= Version("3.0.0"):
        retry_number = 1
    else:
        retry_number = 2

    if watcher_retry_queue and task_instance.try_number and _is_watcher_sensor(task_instance):
        if task_instance.try_number >= retry_number:
            log.info(
                f"Setting task {task_instance.task_id} to use watcher retry queue: {watcher_retry_queue}",
            )
            task_instance.queue = watcher_retry_queue
