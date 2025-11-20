from __future__ import annotations

import logging
from typing import Any, Callable

from packaging.version import Version

ProducerStateFetcher = Callable[[], str | None]


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

        def fetch_state() -> str | None:
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

        return fetch_state

    try:
        RuntimeTaskInstance = _load_airflow3_dependencies()
    except (ImportError, NameError) as exc:  # pragma: no cover - Airflow 3 libs missing
        logger.warning("Could not load Airflow 3 RuntimeTaskInstance: %s", exc)
        return None

    def fetch_state() -> str | None:
        task_states = RuntimeTaskInstance.get_task_states(
            dag_id=dag_id,
            task_ids=[producer_task_id],
            run_ids=[run_id],
        )
        state = task_states.get(run_id, {}).get(producer_task_id)
        if state is not None:
            return str(state)
        return None

    return fetch_state
