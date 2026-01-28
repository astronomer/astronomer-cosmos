from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

from airflow.policies import hookimpl

log = getLogger(__name__)


def _is_watcher_sensor(task_instance: TaskInstance) -> bool:
    log.info(
        f"Checking if task {task_instance.task_id} is a watcher sensor",
    )

    # Avoid circular import by checking class name instead of isinstance
    task_class_name = task_instance.task.__class__.__name__
    task_module = task_instance.task.__class__.__module__

    is_consumer_sensor = task_class_name == "BaseConsumerSensor" or "BaseConsumerSensor" in [
        base.__name__ for base in task_instance.task.__class__.__mro__
    ]

    log.info(
        f"Task is a consumer sensor: {is_consumer_sensor} (class: {task_class_name}, module: {task_module})",
    )

    return is_consumer_sensor


@hookimpl
def task_instance_mutation_hook(task_instance: TaskInstance):

    # from airflow.configuration import conf

    # watcher_retry_queue = conf.get("cosmos", "watcher_retry_queue", fallback=None)

    if task_instance.try_number and _is_watcher_sensor(task_instance):
        if task_instance.try_number >= 1:
            task_instance.queue = "test"
