from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun

from cosmos import telemetry
from cosmos.log import get_logger

logger = get_logger(__name__)


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


DAG_RUN = "dag_run"


def is_cosmos_dag(dag: DAG) -> bool:
    logger.info(f"is_cosmos_dag: {dag.__class__}")
    if dag.__class__.__module__.startswith("cosmos."):
        return True
    return False


def total_cosmos_task_groups(dag: DAG) -> int:
    cosmos_task_groups = 0
    for group_id, task_group in dag.task_group_dict.items():
        logger.info(f"total_cosmos_task_groups: {task_group.__class__}")
        if task_group.__class__.__module__.startswith("cosmos."):
            cosmos_task_groups += 1
    return cosmos_task_groups


def total_cosmos_tasks(dag: DAG) -> int:
    cosmos_tasks = 0
    for task in dag.tasks:
        logger.info(f"total_cosmos_task_groups: {task.__class__}")
        if task.__class__.__module__.startswith("cosmos."):
            cosmos_tasks += 1
    return cosmos_tasks


def uses_cosmos(dag: DAG) -> bool:
    return bool(is_cosmos_dag(dag) or total_cosmos_task_groups(dag) or total_cosmos_tasks(dag))


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str) -> None:
    logger.info("The on_dag_run_success was called")

    dag = dag_run.get_dag()

    logger.info(f"dir: {dir(dag)}")
    logger.info(f"1: {dag.fileloc}")
    logger.info(f"2:{dag.filepath}")
    logger.info(f"3: {dag.task_dict}")
    logger.info(f"4: {dag.task_group_dict}")
    logger.info(f"5: {dag.get_serialized_fields()}")
    logger.info(f"6: {dag.to_dict()}")
    logger.info(f"7: {dag.deserialize()}")

    if not isinstance(dag, DAG):
        from airflow.models import DagBag

        logger.info("The DAG does not use Cosmos")
        dag_bag = DagBag(dag_folder=dag.fileloc, include_examples=False)
        dag = dag_bag.get_dag(dag_run.dag_id)

    if not uses_cosmos(dag):
        return

    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
        "task_count": len(dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(dag),
        "cosmos_task_groups_count": total_cosmos_task_groups(dag),
        "is_cosmos_dag": is_cosmos_dag(dag),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
    logger.info("Completed on_dag_run_success")


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str) -> None:
    logger.info("The on_dag_run_failed was called")

    dag = dag_run.get_dag()
    if not uses_cosmos(dag):
        return

    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.FAILED,
        "task_count": len(dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(dag),
        "cosmos_task_groups_count": total_cosmos_task_groups(dag),
        "is_cosmos_dag": is_cosmos_dag(dag),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
    logger.info("Completed on_dag_run_failed")
