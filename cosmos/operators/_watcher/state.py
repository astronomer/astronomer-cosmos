from __future__ import annotations

import logging
from collections.abc import Callable
from threading import Lock
from typing import Any

try:
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as TaskInstance
except ImportError:
    from airflow.models.taskinstance import TaskInstance  # type: ignore[assignment]

from packaging.version import Version

ProducerStateFetcher = Callable[[], str | None]

# dbt uses different status values for different node types (models/tests):"
DBT_SUCCESS_STATUSES = frozenset({"success", "pass"})
DBT_FAILED_STATUSES = frozenset({"failed", "fail", "error"})


def is_dbt_node_status_success(status: str | None) -> bool:
    """Check if the dbt node status indicates success (works for both models and tests)."""
    return status in DBT_SUCCESS_STATUSES


def is_dbt_node_status_failed(status: str | None) -> bool:
    """Check if the dbt node status indicates failure (works for both models and tests)."""
    return status in DBT_FAILED_STATUSES


def is_dbt_node_status_terminal(status: str | None) -> bool:
    """Check if the dbt node status is terminal (success or failed)."""
    return is_dbt_node_status_success(status) or is_dbt_node_status_failed(status)


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


# TODO: Unify the Airflow call from cosmos/operators/_watcher/triggerer.py and cosmos/operators/watcher.py
def get_xcom_val(task_instance: TaskInstance, task_ids: str | list[str], key: str) -> Any:
    return task_instance.xcom_pull(task_ids, key=key)


def _load_airflow2_dependencies() -> tuple[Any, Callable[[], Any]]:
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session

    return TaskInstance, create_session


def _load_airflow3_dependencies() -> Any:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    return RuntimeTaskInstance


def build_producer_state_fetcher(
    *,
    airflow_version: Version,
    dag_id: str,
    run_id: str,
    producer_task_id: str,
    logger: logging.Logger,
) -> ProducerStateFetcher | None:
    """Return a callable that fetches the producer task state for the given Airflow version."""

    if airflow_version < Version("3.0.0"):
        try:
            TaskInstance, create_session = _load_airflow2_dependencies()
        except ImportError as exc:  # pragma: no cover - defensive guard for stripped test envs
            logger.warning("Could not import Airflow 2 state dependencies: %s", exc)
            return None

        def fetch_state_airflow2() -> str | None:
            with create_session() as session:
                ti = (
                    session.query(TaskInstance)
                    .filter_by(
                        dag_id=dag_id,
                        task_id=producer_task_id,
                        run_id=run_id,
                    )
                    .one_or_none()
                )
                if ti is not None:
                    return str(ti.state)
                return None

        return fetch_state_airflow2

    try:
        RuntimeTaskInstance = _load_airflow3_dependencies()
    except (ImportError, NameError) as exc:  # pragma: no cover - Airflow 3 libs missing
        logger.warning("Could not load Airflow 3 RuntimeTaskInstance: %s", exc)
        return None

    def fetch_state_airflow3() -> str | None:
        try:
            task_states = RuntimeTaskInstance.get_task_states(
                dag_id=dag_id,
                task_ids=[producer_task_id],
                run_ids=[run_id],
            )
        except NameError as exc:  # pragma: no cover - Airflow 3.0 missing supervisor comms
            logger.warning("RuntimeTaskInstance.get_task_states unavailable due to NameError: %s", exc)
            return None
        state = task_states.get(run_id, {}).get(producer_task_id)
        if state is not None:
            return str(state)
        return None

    return fetch_state_airflow3
