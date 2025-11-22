from threading import Lock
from typing import Any

try:
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as TaskInstance
except ImportError:
    from airflow.models.taskinstance import TaskInstance  # type: ignore[assignment]


xcom_set_lock = Lock()


def safe_xcom_push(task_instance: TaskInstance, key: str, value: Any) -> None:
    """
    Safely set an XCom value in a thread-safe manner in Airflow 3.0 and 3.1.
    We noticed that the combination of using dbt (multi-threaded) and Airflow 3.0 and 3.1 to set XCom lead to race conditions.
    This leads the producer task to get stuck while running the dbt build command.
    Unfortunately, since this is non-deterministic, and happens once every five runs, we were not able to have a proper test.
    However, we applied this fix and run over 20 times a pipeline that would fail every 5 runs and this allowed us to no longer face the issue.
    """
    with xcom_set_lock:
        task_instance.xcom_push(key=key, value=value)
