"""Utilities supporting :attr:`~cosmos.constants.SeedRenderingBehavior.WHEN_SEED_CHANGES`.

These helpers detect whether a dbt seed's content has changed since the last successful run, so Cosmos
can skip re-running unchanged seeds. The last-seen content checksum is persisted as an Airflow Variable,
scoped per ``DbtDag``/``DbtTaskGroup`` and seed, so the same seed rendered in different DAGs tracks its
state independently (a DAG never wrongly skips a seed because a different DAG ran it).

All functions in this module are private to Cosmos and are not part of the public API.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import Variable
from sqlalchemy.exc import SQLAlchemyError

from cosmos.log import get_logger

logger = get_logger(__name__)

# Airflow Variable keys are length-bounded (the ``variable.key`` column is commonly 250 chars). We store
# the checksum under a fixed-length key: a readable prefix plus a stable hash of the full identifier, so
# long DAG ids / nested task groups / package+resource names can never overflow the column.
_VARIABLE_KEY_PREFIX = "cosmos_seed_checksum"


def _seed_variable_key(dag_task_group_identifier: str, unique_id: str) -> str:
    """Return a bounded, stable Airflow Variable key scoped to a DbtDag/DbtTaskGroup and seed."""
    digest = hashlib.sha1(f"{dag_task_group_identifier}:{unique_id}".encode()).hexdigest()
    return f"{_VARIABLE_KEY_PREFIX}__{digest}"


def _compute_csv_checksum(seed_file_path: str | Path) -> str | None:
    """Return the SHA256 checksum of a seed CSV file, or ``None`` if it cannot be read."""
    try:
        content = Path(seed_file_path).read_bytes()
    except OSError as exc:
        logger.warning("Unable to read seed file `%s` for change detection: %s", seed_file_path, exc)
        return None
    return hashlib.sha256(content).hexdigest()


def _resolve_seed_checksum(manifest_checksum: str | None, seed_file_path: str | None) -> str | None:
    """Resolve the seed's current checksum, preferring dbt's manifest checksum over hashing the CSV.

    dbt records a content checksum per node in ``manifest.json`` (for seeds, the sha256 of the CSV), which
    matches dbt's own ``state:modified`` semantics. When loading via ``dbt ls`` the checksum is unavailable,
    so we fall back to hashing the CSV file directly on the worker.
    """
    if manifest_checksum:
        return manifest_checksum
    if seed_file_path:
        return _compute_csv_checksum(seed_file_path)
    return None


def _evaluate_seed_change(
    dag_task_group_identifier: str | None,
    unique_id: str | None,
    manifest_checksum: str | None,
    seed_file_path: str | None,
) -> tuple[bool, str | None, str | None]:
    """Decide whether a seed should run, based on whether its content changed since the last successful run.

    :returns: ``(should_run, checksum, variable_key)``. ``should_run`` is ``False`` only when the seed's
        current checksum positively matches the stored one. ``checksum`` and ``variable_key`` are returned
        so the caller can persist the value after a successful run without recomputing it; both are ``None``
        when the change could not be determined, in which case the seed should run.
    """
    current = _resolve_seed_checksum(manifest_checksum, seed_file_path)
    if current is None or not dag_task_group_identifier or not unique_id:
        logger.info(
            "Seed change detection could not resolve a checksum for seed `%s` (checksum resolved: %s); "
            "running `dbt seed` without change detection.",
            unique_id,
            current is not None,
        )
        return True, None, None

    variable_key = _seed_variable_key(dag_task_group_identifier, unique_id)
    stored = Variable.get(variable_key, default_var=None)
    return stored != current, current, variable_key


def _persist_seed_checksum(variable_key: str | None, checksum: str | None) -> None:
    """Persist the seed's checksum after a successful run.

    Best-effort: a failure to store the checksum must never fail a seed that already loaded successfully,
    so storage errors are logged and swallowed rather than raised.
    """
    if not variable_key or checksum is None:
        return
    try:
        Variable.set(variable_key, checksum)
    except (AirflowException, SQLAlchemyError) as exc:
        logger.warning("Failed to persist seed checksum under Variable `%s`: %s", variable_key, exc)
