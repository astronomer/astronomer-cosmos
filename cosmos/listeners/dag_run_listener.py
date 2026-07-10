from __future__ import annotations

import binascii
import hashlib
import json
import zlib
from typing import TYPE_CHECKING, Any

from airflow.listeners import hookimpl

if TYPE_CHECKING:
    try:
        from airflow.sdk import DAG
    except ImportError:
        from airflow.models.dag import DAG  # type: ignore[assignment]
    from airflow.models.dagrun import DagRun

from cosmos import telemetry
from cosmos.constants import _AIRFLOW3_MAJOR_VERSION, AIRFLOW_VERSION
from cosmos.log import get_logger
from cosmos.telemetry import _decompress_telemetry_metadata

AIRFLOW_VERSION_MAJOR = AIRFLOW_VERSION.major

logger = get_logger(__name__)


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


DAG_RUN = "dag_run"


_WATCHER_PRODUCER_TASK_TYPE = "DbtProducerWatcherOperator"


def _cleanup_watcher_producer_backups(dag: DAG, dag_id: str, run_id: str) -> None:
    """Delete any orphaned WATCHER producer XCom-backup Variables left by this DAG run.

    Called from both ``on_dag_run_success`` and ``on_dag_run_failed``: a ``DbtDag``'s root
    consumer tasks run with ``trigger_rule="always"``, so the run can succeed even if the
    producer itself failed and never got to clean up after itself. Either way, the DAG run is
    already terminal by the time either hook fires, so no further producer retries are possible
    and it's always safe to delete.
    """
    from cosmos.operators._watcher.xcom import _delete_xcom_backup_variable_by_ids

    for task in dag.task_dict.values():
        task_module = getattr(task, "_task_module", None) or task.__class__.__module__
        # NOT `_task_type`: that attribute doesn't exist on Airflow 3's SerializedBaseOperator.
        task_type = getattr(task, "task_type", None) or task.__class__.__name__
        if task_module.startswith("cosmos.") and task_type == _WATCHER_PRODUCER_TASK_TYPE:
            task_group = getattr(task, "task_group", None)
            task_group_id = task_group.group_id if task_group else None
            try:
                _delete_xcom_backup_variable_by_ids(dag_id, task_group_id, run_id)
            except Exception:
                logger.warning(
                    "Failed to clean up WATCHER producer XCom-backup Variable for task '%s'",
                    task.task_id,
                    exc_info=True,
                )


def _cleanup_watcher_producer_backups_safely(dag: DAG, dag_run: DagRun) -> None:
    try:
        _cleanup_watcher_producer_backups(dag, dag_run.dag_id, dag_run.run_id)
    except Exception:
        logger.warning("Failed to clean up WATCHER producer XCom-backup Variables", exc_info=True)


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


def get_execution_modes(dag: DAG) -> str:
    """Determine the execution mode(s) based on task modules in the DAG."""
    modes = {
        (getattr(task, "_task_module", None) or task.__class__.__module__).split(".")[2]
        for task in dag.task_dict.values()
        if (getattr(task, "_task_module", None) or task.__class__.__module__).startswith("cosmos.")
    }

    # Sorted to ensure consistent and predictable output
    return "__".join(sorted(modes))


def get_cosmos_telemetry_metadata(dag: DAG) -> dict[str, Any]:
    """
    Extract Cosmos telemetry metadata from a DAG.

    Returns the metadata dictionary stored by the converter in dag.params, or an empty dict if not present.
    """
    # Metadata is stored as compressed string in dag.params to survive serialization
    compressed_metadata = dag.params.get("__cosmos_telemetry_metadata__")

    if not compressed_metadata:
        return {}

    try:
        return _decompress_telemetry_metadata(compressed_metadata)
    except (binascii.Error, zlib.error, json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Failed to decompress telemetry metadata: %s: %s", type(e).__name__, e)
        return {}


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

    if AIRFLOW_VERSION_MAJOR < _AIRFLOW3_MAJOR_VERSION:
        dag_hash = dag_run.dag_hash
    else:
        dag_hash = hashlib.md5(dag_run.dag_id.encode("utf-8")).hexdigest()

    additional_telemetry_metrics = {
        "dag_hash": dag_hash,
        "status": EventStatus.SUCCESS,
        "task_count": len(serialized_dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(serialized_dag),
        "execution_modes": get_execution_modes(serialized_dag),
    }

    # Add Cosmos telemetry metadata if available
    cosmos_metadata = get_cosmos_telemetry_metadata(serialized_dag)
    additional_telemetry_metrics.update(cosmos_metadata)

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)

    _cleanup_watcher_producer_backups_safely(serialized_dag, dag_run)

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

    if AIRFLOW_VERSION_MAJOR < _AIRFLOW3_MAJOR_VERSION:
        dag_hash = dag_run.dag_hash
    else:
        dag_hash = hashlib.md5(dag_run.dag_id.encode("utf-8")).hexdigest()

    additional_telemetry_metrics = {
        "dag_hash": dag_hash,
        "status": EventStatus.FAILED,
        "task_count": len(serialized_dag.task_ids),
        "cosmos_task_count": total_cosmos_tasks(serialized_dag),
        "execution_modes": get_execution_modes(serialized_dag),
    }

    # Add Cosmos telemetry metadata if available
    cosmos_metadata = get_cosmos_telemetry_metadata(serialized_dag)
    additional_telemetry_metrics.update(cosmos_metadata)

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)

    _cleanup_watcher_producer_backups_safely(serialized_dag, dag_run)

    logger.debug("Completed on_dag_run_failed")
