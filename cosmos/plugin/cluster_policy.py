from logging import getLogger

from airflow.models.taskinstance import TaskInstance
from airflow.policies import hookimpl
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from cosmos.operators._watcher.base import BaseConsumerSensor
from cosmos.settings import watcher_retry_queue

log = getLogger(__name__)


def _is_watcher_sensor(task_instance: TaskInstance) -> bool:
    log.info(
        f"Checking if task {task_instance.task_id} is a watcher sensor",
    )

    is_consumer_sensor = isinstance(task_instance.task, BaseConsumerSensor)

    log.info(
        f"Task {task_instance.task_id} is a consumer sensor: {is_consumer_sensor}",
    )

    return is_consumer_sensor


@hookimpl
def task_instance_mutation_hook(task_instance: TaskInstance) -> None:

    # In Airflow 3.x the task_instance_mutation_hook try_number starts at None or 0
    # in Airflow 2.x it starts at 1
    if AIRFLOW_VERSION < Version("3.0.0"):
        first_try_number = 1
    else:
        first_try_number = 2

    if task_instance.try_number and _is_watcher_sensor(task_instance):
        log.info(f"CLUSTER POLICY: {task_instance.task_id} try number: {task_instance.try_number}")
        if task_instance.try_number >= first_try_number and watcher_retry_queue:
            log.info(
                f"CLUSTER POLICY: Setting task {task_instance.task_id} to use watcher retry queue: {watcher_retry_queue}",
            )
            task_instance.queue = watcher_retry_queue
