from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.hashlib_wrapper import md5

from cosmos import telemetry
from cosmos.log import get_logger

logger = get_logger(__name__)


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


DAG_RUN = "dag_run"


def total_cosmos_tasks(dag: DAG) -> int:
    """
    Identify if there are any Cosmos DAGs on a given serialized `airflow.serialization.serialized_objects.SerializedDAG`.

    The approach is naive, from the perspective it does not take into account subclasses, but it is inexpensive and
    works.
    """
    cosmos_tasks = 0
    for task in dag.task_dict.values():
        # In a real Airflow deployment, the following `task` is an instance of
        # `airflow.serialization.serialized_objects.SerializedBaseOperator`
        # and the only reference to Cosmos is in the _task_module.
        # It is suboptimal, but works as of Airflow 2.10
        task_module = getattr(task, "_task_module", None) or task.__class__.__module__
        if task_module.startswith("cosmos."):
            cosmos_tasks += 1
    return cosmos_tasks


# @provide_session
@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str) -> None:
    logger.debug("Running on_dag_run_success")
    # In a real Airflow deployment, the following `serialized_dag` is an instance of
    # `airflow.serialization.serialized_objects.SerializedDAG`
    # and it is not a subclass of DbtDag, nor contain any references to Cosmos
    serialized_dag = dag_run.get_dag()

    if not total_cosmos_tasks(serialized_dag):
        logger.debug("The DAG does not use Cosmos")
        return

    additional_telemetry_metrics = {
        "dag_hash": md5(dag_run.dag_id.encode("utf-8")).hexdigest(),
        "status": EventStatus.SUCCESS,
        "task_count": len(serialized_dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(serialized_dag),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
    logger.debug("Completed on_dag_run_success")


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str) -> None:
    logger.debug("Running on_dag_run_failed")
    # In a real Airflow deployment, the following `serialized_dag` is an instance of
    # `airflow.serialization.serialized_objects.SerializedDAG`
    # and it is not a subclass of DbtDag, nor contain any references to Cosmos
    serialized_dag = dag_run.get_dag()

    if not total_cosmos_tasks(serialized_dag):
        logger.debug("The DAG does not use Cosmos")
        return

    additional_telemetry_metrics = {
        "dag_hash": md5(dag_run.dag_id.encode("utf-8")).hexdigest(),
        "status": EventStatus.FAILED,
        "task_count": len(serialized_dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(serialized_dag),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
    logger.debug("Completed on_dag_run_failed")
