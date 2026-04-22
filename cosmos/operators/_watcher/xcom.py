"""XCom backup/restore logic for watcher producer retry resilience.

When the watcher producer fails and retries, Airflow clears XCom entries from
the previous attempt.  This module provides an incremental backup mechanism
that persists XCom key/value pairs to an Airflow Variable so they can be
restored on retry.
"""

from __future__ import annotations

import base64
import json
import zlib
from typing import Any

try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable  # type: ignore[no-redef]

from cosmos.log import get_logger
from cosmos.operators._watcher.state import safe_xcom_push

logger = get_logger(__name__)

XCOM_BACKUP_VARIABLE_PREFIX = "cosmos_xcom_backup"


def _xcom_backup_variable_key(dag_id: str, task_group_id: str | None, run_id: str) -> str:
    """Build a unique Airflow Variable key for the XCom backup of a watcher producer run."""
    parts = [XCOM_BACKUP_VARIABLE_PREFIX, dag_id.replace(".", "___")]
    if task_group_id:
        parts.append(task_group_id.replace(".", "__"))
    parts.append(run_id.replace(".", "_"))
    return "__".join(parts)


def _get_task_group_id(ti: Any) -> str | None:
    """Extract the task_group_id from a task instance, if available."""
    task = getattr(ti, "task", None)
    return getattr(task, "task_group_id", None) if task else None


def _init_xcom_backup(context: Any) -> None:
    """Activate incremental XCom backup for the current producer execution.

    After this call every ``safe_xcom_push`` will also persist the key/value
    pair to an Airflow Variable so the backup is crash-safe.  The state is
    stored on the task instance itself — no module-level globals.
    """
    ti = context["ti"]
    dag_id = ti.dag_id
    run_id = context["run_id"]
    task_group_id = _get_task_group_id(ti)
    ti._cosmos_xcom_backup_var_key = _xcom_backup_variable_key(dag_id, task_group_id, run_id)  # type: ignore[attr-defined]
    ti._cosmos_xcom_backup_buffer = {}  # type: ignore[attr-defined]


def _persist_backup(var_key: str, backup_buffer: dict[str, Any]) -> None:
    """Write the current backup buffer to an Airflow Variable."""
    if not backup_buffer:
        return

    compressed = base64.b64encode(zlib.compress(json.dumps(backup_buffer, default=str).encode("utf-8"))).decode("utf-8")
    Variable.set(var_key, compressed)
    logger.debug("Persisted %d XCom entries to Variable '%s'", len(backup_buffer), var_key)


def _backup_xcom_to_variable(context: Any) -> None:
    """Persist all XCom entries for the current producer task in an Airflow Variable.

    The backup is built incrementally by ``safe_xcom_push`` (see
    ``_init_xcom_backup``).  This call does a final flush.
    """
    ti = context["ti"]
    var_key = getattr(ti, "_cosmos_xcom_backup_var_key", None)
    if not isinstance(var_key, str):
        return

    backup_buffer = getattr(ti, "_cosmos_xcom_backup_buffer", {})
    _persist_backup(var_key, backup_buffer)
    if backup_buffer:
        logger.debug("Backed up %d XCom entries to Variable '%s'", len(backup_buffer), var_key)


def _delete_xcom_backup_variable(context: Any) -> None:
    """Delete the XCom backup Variable after a successful producer execution."""
    ti = context["ti"]
    var_key = getattr(ti, "_cosmos_xcom_backup_var_key", None)
    if not isinstance(var_key, str):
        return
    try:
        Variable.delete(var_key)
        logger.debug("Deleted XCom backup Variable '%s'", var_key)
    except KeyError:
        pass


def _restore_xcom_from_variable(context: Any) -> bool:
    """Restore XCom entries from an Airflow Variable backup created by a previous attempt.

    Returns True if the restore succeeded, False if no backup was found.
    """
    ti = context["ti"]
    dag_id = ti.dag_id
    run_id = context["run_id"]
    task_group_id = _get_task_group_id(ti)

    var_key = _xcom_backup_variable_key(dag_id, task_group_id, run_id)
    try:
        compressed = Variable.get(var_key, default_var=None)
    except TypeError:
        # airflow.sdk.Variable.get() does not support default_var
        try:
            compressed = Variable.get(var_key)
        except Exception:
            compressed = None
    if compressed is None:
        logger.info("No XCom backup Variable found at '%s'", var_key)
        return False

    backup: dict[str, Any] = json.loads(zlib.decompress(base64.b64decode(compressed.encode("utf-8"))).decode("utf-8"))
    for key, value in backup.items():
        safe_xcom_push(task_instance=ti, key=key, value=value)
    logger.info("Restored %d XCom entries from Variable '%s'", len(backup), var_key)

    try:
        Variable.delete(var_key)
        logger.debug("Deleted XCom backup Variable '%s' after restore", var_key)
    except KeyError:
        logger.debug("XCom backup Variable '%s' already deleted", var_key)
    return True
