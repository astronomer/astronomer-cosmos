from logging import getLogger

from airflow.configuration import conf
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
def task_instance_mutation_hook(task_instance: TaskInstance) -> None:
    """
    Set custom queue for Cosmos tasks on retry.
    
    Configure the retry queue via:
    [cosmos]
    retry_queue = <queue_name>
    """
    # Only apply on retries (try_number >= 2 means it's a retry attempt)
    if not task_instance.try_number or task_instance.try_number < 2:
        return
    
    if not _is_watcher_sensor(task_instance):
        return
    
    retry_queue = conf.get("cosmos", "watcher_retry_queue", fallback=None)
    if retry_queue:
        log.info(
            f"Setting retry queue '{retry_queue}' for Cosmos task {task_instance.task_id} "
            f"(try_number: {task_instance.try_number})",
        )
        task_instance.queue = retry_queue
    else:
        log.info(
            f"No retry_queue configured for Cosmos task {task_instance.task_id}. "
            "Set [cosmos] retry_queue in airflow.cfg to enable custom retry queue.",
        )
