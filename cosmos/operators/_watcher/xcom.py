"""XCom backup/restore logic for watcher producer retry resilience.

When the watcher producer fails and retries, Airflow clears XCom entries from
the previous attempt.  This module provides an incremental backup mechanism
that persists XCom key/value pairs to the Airflow task store (Airflow 3.3+)
or to an Airflow Variable so they can be restored on retry.
"""

from __future__ import annotations

import base64
import json
import re
import zlib
from typing import Any

from packaging.version import Version

from cosmos.airflow.compatibility import delete_variable, get_variable, set_variable
from cosmos.constants import AIRFLOW_VERSION
from cosmos.log import get_logger
from cosmos.operators._watcher.state import safe_xcom_push

logger = get_logger(__name__)

XCOM_BACKUP_VARIABLE_PREFIX = "cosmos_xcom_backup"
TASK_STATE_STORE_BACKUP_KEY = "cosmos_xcom_backup"
# Airflow warns about task store values above [state_store] max_value_storage_bytes (65535 by default).
MAX_BACKUP_ROW_BYTES = 8 * 1024
TASK_STATE_STORE_CONTEXT_KEY = "task_state_store"
TASK_STATE_STORE_TI_ATTRIBUTE = "_cosmos_task_state_store"

# Characters that secrets backends commonly reject in Variable keys. AWS
# Secrets Manager allows alphanumerics + ``-/_+=.@!``; GCP Secret Manager is
# stricter at ``[A-Za-z0-9_-]``. Run IDs routinely contain ``:`` and ``+``
# (timestamps and timezone offsets, e.g. ``scheduled__2026-05-04T10:15:00+00:00``)
# and dag/task-group IDs can contain ``.``. Sanitize all components down to
# ``[A-Za-z0-9_-]`` so the resulting key is portable across backends and does
# not log a ``ValidationException`` on every ``Variable.set`` call when an
# external secrets backend is configured (Airflow walks the backend chain on
# set as well as get).
_DISALLOWED_VARIABLE_KEY_CHAR_RE = re.compile(r"[^A-Za-z0-9_-]")


def _xcom_backup_variable_key(dag_id: str, task_group_id: str | None, run_id: str) -> str:
    """Build a unique Airflow Variable key for the XCom backup of a watcher producer run.

    The component-specific period-replacement counts (3 underscores for dag_id,
    2 for task_group_id, 1 for run_id) are preserved so keys remain visually
    parseable, then any remaining disallowed character is normalized to ``_``.
    """
    parts = [XCOM_BACKUP_VARIABLE_PREFIX, _sanitize_key_component(dag_id.replace(".", "___"))]
    if task_group_id:
        parts.append(_sanitize_key_component(task_group_id.replace(".", "__")))
    parts.append(_sanitize_key_component(run_id.replace(".", "_")))
    return "__".join(parts)


def _sanitize_key_component(value: str) -> str:
    """Replace characters that secrets backends reject in Variable keys with ``_``."""
    return _DISALLOWED_VARIABLE_KEY_CHAR_RE.sub("_", value)


def _get_task_group_id(ti: Any) -> str | None:
    """Extract the enclosing task group's id from a task instance, if available.

    Airflow operators expose the parent group via ``task.task_group``, and
    ``TaskGroup.group_id`` is the canonical identifier on both Airflow 2 and
    Airflow 3. Direct attribute access is intentional: a previous version of
    this helper read ``task.task_group_id`` (an attribute that does not exist
    on operators) via ``getattr`` with a ``None`` default, silently dropping
    the task-group segment from the XCom backup Variable key and causing every
    watcher producer in the same DAG run to share one key. See
    https://github.com/astronomer/astronomer-cosmos/issues/2625.
    """
    task = getattr(ti, "task", None)
    if task is None:
        return None
    # On a bound operator ``task.task_group`` is always a ``TaskGroup``: the
    # implicit root group (whose ``group_id`` is ``None``) for top-level tasks,
    # or the enclosing user-defined group otherwise.
    group_id: str | None = task.task_group.group_id
    return group_id


def _init_xcom_backup(context: Any, *, persist: bool = True) -> None:
    """Set up the producer's in-memory backup buffer.

    With ``persist`` the buffer is written after every push, to the Airflow task store on Airflow 3.3+ and to an
    Airflow Variable before that. Without it the buffer is flushed to a Variable once, from the producer's retry
    callback.
    """
    ti = context["ti"]
    ti._cosmos_xcom_backup_buffer = {}
    # Writes to the task store from the retry callback are rejected, so it only serves the per-push backup.
    task_state_store = None  # bench: Cosmos 1.15 behaviour, the per-push backup goes to an Airflow Variable
    setattr(ti, TASK_STATE_STORE_TI_ATTRIBUTE, task_state_store)
    ti._cosmos_xcom_backup_open_row = {}
    ti._cosmos_xcom_backup_rows_written = 0
    ti._cosmos_xcom_persist_incrementally = persist
    dag_id = ti.dag_id
    run_id = context["run_id"]
    task_group_id = _get_task_group_id(ti)
    ti._cosmos_xcom_backup_var_key = _xcom_backup_variable_key(dag_id, task_group_id, run_id)


def _encode_backup(entries: dict[str, Any]) -> str:
    return base64.b64encode(zlib.compress(json.dumps(entries, default=str).encode("utf-8"))).decode("utf-8")


def _decode_backup(compressed: str) -> dict[str, Any]:
    backup: dict[str, Any] = json.loads(zlib.decompress(base64.b64decode(compressed.encode("utf-8"))).decode("utf-8"))
    return backup


def _task_state_store_row_key(row_index: int) -> str:
    return f"{TASK_STATE_STORE_BACKUP_KEY}_{row_index}"


def _persist_backup(var_key: str, backup_buffer: dict[str, Any]) -> None:
    """Write the whole backup buffer to an Airflow Variable."""
    if not backup_buffer:
        return
    set_variable(var_key, _encode_backup(backup_buffer))
    logger.debug("Persisted %d XCom entries to Variable '%s'", len(backup_buffer), var_key)


def _persist_backup_row(ti: Any, key: str, value: Any) -> None:
    """Add one pushed entry to the open task store row, starting a new row once it reaches MAX_BACKUP_ROW_BYTES."""
    task_state_store = getattr(ti, TASK_STATE_STORE_TI_ATTRIBUTE)
    open_row: dict[str, Any] = ti._cosmos_xcom_backup_open_row
    open_row[key] = value
    encoded = _encode_backup(open_row)
    row_index = ti._cosmos_xcom_backup_rows_written
    task_state_store.set(_task_state_store_row_key(row_index), encoded)
    if len(encoded) >= MAX_BACKUP_ROW_BYTES:
        ti._cosmos_xcom_backup_rows_written = row_index + 1
        ti._cosmos_xcom_backup_open_row = {}


def _read_task_state_store_rows(task_state_store: Any) -> tuple[dict[str, Any], int]:
    """Read the backup rows in order, so an entry pushed twice keeps its latest value; return them and the row count."""
    entries: dict[str, Any] = {}
    row_index = 0
    while (compressed := task_state_store.get(_task_state_store_row_key(row_index))) is not None:
        entries.update(_decode_backup(compressed))
        row_index += 1
    return entries, row_index


def _delete_task_state_store_rows(task_state_store: Any, row_count: int) -> None:
    for row_index in range(row_count):
        task_state_store.delete(_task_state_store_row_key(row_index))


def _backup_xcom_to_variable(context: Any) -> None:
    """Flush the producer's backup buffer to an Airflow Variable from the retry or failure callback.

    The buffer is built by ``safe_xcom_push`` (see ``_init_xcom_backup``). Nothing is written when the backup
    is kept in the task store, which already holds every push.
    """
    ti = context["ti"]
    var_key = getattr(ti, "_cosmos_xcom_backup_var_key", None)
    if not isinstance(var_key, str) or getattr(ti, TASK_STATE_STORE_TI_ATTRIBUTE, None) is not None:
        return

    backup_buffer = getattr(ti, "_cosmos_xcom_backup_buffer", {})
    _persist_backup(var_key, backup_buffer)
    if backup_buffer:
        logger.debug("Backed up %d XCom entries to Variable '%s'", len(backup_buffer), var_key)


def _compose_backup_callback(existing: Any) -> Any:
    """Append ``_backup_xcom_to_variable`` to any existing callback(s), idempotently (used for on_retry_callback and on_failure_callback)."""
    if existing is None:
        return _backup_xcom_to_variable
    callbacks = list(existing) if isinstance(existing, (list, tuple)) else [existing]
    if _backup_xcom_to_variable not in callbacks:
        callbacks.append(_backup_xcom_to_variable)
    return callbacks


def _delete_xcom_backup_variable(context: Any) -> None:
    """Delete the XCom backup, in the task store or the Variable, after a successful producer execution."""
    ti = context["ti"]
    task_state_store = getattr(ti, TASK_STATE_STORE_TI_ATTRIBUTE, None)
    if task_state_store is not None:
        _delete_task_state_store_rows(task_state_store, ti._cosmos_xcom_backup_rows_written + 1)
        return
    var_key = getattr(ti, "_cosmos_xcom_backup_var_key", None)
    if not isinstance(var_key, str):
        return
    try:
        delete_variable(var_key)
        logger.debug("Deleted XCom backup Variable '%s'", var_key)
    except KeyError:
        pass


def _restore_xcom_from_variable(context: Any) -> bool:
    """Restore XCom entries from the backup a previous attempt left in the task store or in an Airflow Variable.

    Returns True if the restore succeeded, False if no backup was found.
    """
    ti = context["ti"]
    task_state_store = context.get(TASK_STATE_STORE_CONTEXT_KEY)
    if task_state_store is not None:
        entries, row_count = _read_task_state_store_rows(task_state_store)
        if row_count:
            _push_entries(ti, entries)
            _delete_task_state_store_rows(task_state_store, row_count)
            return True
        logger.info("No XCom backup found in the task store, checking the backup Variable")

    dag_id = ti.dag_id
    run_id = context["run_id"]
    task_group_id = _get_task_group_id(ti)

    var_key = _xcom_backup_variable_key(dag_id, task_group_id, run_id)
    compressed = get_variable(var_key, default=None)
    if compressed is None:
        logger.info("No XCom backup Variable found at '%s'", var_key)
        return False

    _push_entries(ti, _decode_backup(compressed))

    try:
        delete_variable(var_key)
        logger.debug("Deleted XCom backup Variable '%s' after restore", var_key)
    except KeyError:
        logger.debug("XCom backup Variable '%s' already deleted", var_key)
    return True


def _push_entries(ti: Any, entries: dict[str, Any]) -> None:
    for key, value in entries.items():
        safe_xcom_push(task_instance=ti, key=key, value=value)
    logger.info("Restored %d XCom entries from the backup", len(entries))


def _log_missing_backup(try_number: int) -> None:
    message = (
        "No XCom backup found for attempt #%s: the producer skips and the consumers fall back to running their own "
        "dbt nodes."
    )
    if AIRFLOW_VERSION >= Version("3.4.0"):
        message += (
            " Clearing the producer from the Airflow UI or API discards its task store unless 'Keep task state' is "
            "ticked (Settings > Clearing makes it the default)."
        )
    logger.warning(message, try_number)
