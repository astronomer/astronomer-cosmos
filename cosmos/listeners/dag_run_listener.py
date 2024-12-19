from __future__ import annotations

import functools
import time
from contextlib import contextmanager
from typing import Generator

from airflow.listeners import hookimpl
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun

from cosmos import telemetry
from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.log import get_logger

logger = get_logger(__name__)


@contextmanager
def measure_time() -> Generator[None, None, None]:
    start = time.perf_counter()
    yield
    end = time.perf_counter()
    logger.info(f"DAG listener metrics collection time: {end - start:.6f} seconds")


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


DAG_RUN = "dag_run"


@functools.lru_cache()
def is_cosmos_dag(dag: DAG) -> bool:
    if isinstance(dag, DbtDag):
        return True
    return False


@functools.lru_cache()
def total_cosmos_task_groups(dag: DAG) -> int:
    cosmos_task_groups = 0
    for group_id, task_group in dag.task_group_dict.items():
        if isinstance(task_group, DbtTaskGroup):
            cosmos_task_groups += 1
    return cosmos_task_groups


@functools.lru_cache()
def total_cosmos_tasks(dag: DAG) -> int:
    cosmos_tasks = 0
    for task in dag.tasks:
        task_class = type(task)
        task_module = task_class.__module__
        if task_module.startswith("cosmos."):
            cosmos_tasks += 1
    return cosmos_tasks


@functools.lru_cache()
def uses_cosmos(dag: DAG) -> bool:
    return bool(is_cosmos_dag(dag) or total_cosmos_task_groups(dag) or total_cosmos_tasks(dag))


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str) -> None:
    logger.info("The on_dag_run_success was called")
    print("The on_dag_run_success was called")
    dag = dag_run.get_dag()
    if not uses_cosmos(dag):
        return

    with measure_time():
        additional_telemetry_metrics = {
            "dag_hash": dag_run.dag_hash,
            "status": EventStatus.SUCCESS,
            "task_count": len(dag.task_ids),
            "cosmos_task_count": total_cosmos_tasks(dag),
            "cosmos_task_groups_count": total_cosmos_task_groups(dag),
            "is_cosmos_dag": is_cosmos_dag(dag),
        }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str) -> None:
    logger.info("The on_dag_run_failed was called")
    print("The on_dag_run_failed was called")
    dag = dag_run.get_dag()
    if not uses_cosmos(dag):
        return

    with measure_time():
        additional_telemetry_metrics = {
            "dag_hash": dag_run.dag_hash,
            "status": EventStatus.FAILED,
            "task_count": len(dag.task_ids),
            "cosmos_task_count": total_cosmos_tasks(dag),
            "cosmos_task_groups_count": total_cosmos_task_groups(dag),
            "is_cosmos_dag": is_cosmos_dag(dag),
        }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
