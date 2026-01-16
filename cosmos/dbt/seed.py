"""
Module for handling seed change detection using Airflow Variables.

This module provides utilities to compute file hashes and store them in Airflow Variables,
enabling the WHEN_SEED_CHANGES seed rendering behavior.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

try:
    # Airflow 3 onwards
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable

from cosmos.log import get_logger

logger = get_logger(__name__)

# Prefix for Airflow Variables storing seed hashes
SEED_HASH_VARIABLE_PREFIX = "cosmos_seed_hash"


def _compute_file_hash(file_path: Path) -> str:
    """
    Compute the SHA256 hash of a file's contents.

    :param file_path: Path to the file to hash
    :returns: Hexadecimal string of the file's SHA256 hash
    :raises FileNotFoundError: If the file does not exist
    """
    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        # Read in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()


def _get_variable_key(dag_task_group_identifier: str, node_unique_id: str) -> str:
    """
    Generate a unique Airflow Variable key for storing a seed's hash.

    :param dag_task_group_identifier: Identifier for the DAG/TaskGroup (e.g., "my_dag__my_task_group")
    :param node_unique_id: The dbt node's unique_id (e.g., "seed.my_project.my_seed")
    :returns: A unique variable key
    """
    # Sanitize the identifiers to create a valid variable name
    sanitized_dag_id = dag_task_group_identifier.replace(".", "_").replace("-", "_")
    sanitized_node_id = node_unique_id.replace(".", "_").replace("-", "_")
    return f"{SEED_HASH_VARIABLE_PREFIX}__{sanitized_dag_id}__{sanitized_node_id}"


def get_stored_seed_hash(dag_task_group_identifier: str, node_unique_id: str) -> str | None:
    """
    Retrieve the stored hash for a seed from Airflow Variables.

    :param dag_task_group_identifier: Identifier for the DAG/TaskGroup
    :param node_unique_id: The dbt node's unique_id
    :returns: The stored hash string, or None if not found
    """
    variable_key = _get_variable_key(dag_task_group_identifier, node_unique_id)
    try:
        return Variable.get(variable_key, None)
    except Exception as e:
        logger.warning("Failed to retrieve seed hash variable %s: %s", variable_key, e)
        return None


def store_seed_hash(dag_task_group_identifier: str, node_unique_id: str, file_hash: str) -> None:
    """
    Store a seed's file hash in an Airflow Variable.

    :param dag_task_group_identifier: Identifier for the DAG/TaskGroup
    :param node_unique_id: The dbt node's unique_id
    :param file_hash: The hash string to store
    """
    variable_key = _get_variable_key(dag_task_group_identifier, node_unique_id)
    try:
        Variable.set(variable_key, file_hash)
        logger.info("Stored seed hash for %s in variable %s", node_unique_id, variable_key)
    except Exception as e:
        logger.error("Failed to store seed hash variable %s: %s", variable_key, e)
        raise


def has_seed_changed(
    dag_task_group_identifier: str,
    node_unique_id: str,
    seed_file_path: Path | str,
) -> bool:
    """
    Check if a seed's CSV file has changed since the last recorded execution.

    Compares the current file hash with the stored hash in Airflow Variables.

    :param dag_task_group_identifier: Identifier for the DAG/TaskGroup
    :param node_unique_id: The dbt node's unique_id
    :param seed_file_path: Path to the seed's CSV file
    :returns: True if the seed has changed (or no previous hash exists), False otherwise
    """
    seed_path = Path(seed_file_path) if isinstance(seed_file_path, str) else seed_file_path

    if not seed_path.exists():
        logger.warning("Seed file does not exist: %s. Treating as changed.", seed_path)
        return True

    # Compute current hash
    try:
        current_hash = _compute_file_hash(seed_path)
    except Exception as e:
        logger.error("Failed to compute hash for seed file %s: %s. Treating as changed.", seed_path, e)
        return True

    # Get stored hash
    stored_hash = get_stored_seed_hash(dag_task_group_identifier, node_unique_id)

    if stored_hash is None:
        logger.info("No stored hash found for seed %s. Seed will be run.", node_unique_id)
        return True

    if current_hash != stored_hash:
        logger.info(
            "Seed %s has changed (stored hash: %s..., current hash: %s...).",
            node_unique_id,
            stored_hash[:8],
            current_hash[:8],
        )
        return True

    logger.info("Seed %s has not changed (hash: %s...).", node_unique_id, current_hash[:8])
    return False


def update_seed_hash_after_run(
    dag_task_group_identifier: str,
    node_unique_id: str,
    seed_file_path: Path | str,
) -> None:
    """
    Update the stored hash for a seed after successful execution.

    :param dag_task_group_identifier: Identifier for the DAG/TaskGroup
    :param node_unique_id: The dbt node's unique_id
    :param seed_file_path: Path to the seed's CSV file
    """
    seed_path = Path(seed_file_path) if isinstance(seed_file_path, str) else seed_file_path

    if not seed_path.exists():
        logger.warning("Seed file does not exist: %s. Cannot update hash.", seed_path)
        return

    try:
        current_hash = _compute_file_hash(seed_path)
        store_seed_hash(dag_task_group_identifier, node_unique_id, current_hash)
    except Exception as e:
        logger.error("Failed to update seed hash for %s: %s", node_unique_id, e)
        raise
