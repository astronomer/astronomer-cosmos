from __future__ import annotations

import json
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from packaging.version import Version

from cosmos import settings
from cosmos.constants import AIRFLOW_VERSION
from cosmos.log import get_logger

if TYPE_CHECKING:
    from cosmos.config import ProfileConfig

    try:
        from airflow.sdk import DAG  # type: ignore[assignment]

        # Airflow 3.1 onwards
        from airflow.utils.task_group import TaskGroup
    except ImportError:
        from airflow import DAG  # type: ignore[assignment]
        from airflow.utils.task_group import TaskGroup

logger = get_logger(__name__)

# Mapping of dbt adapter type to a callable that derives the OL-compatible dataset namespace
# from the profile dict. Each callable receives the profile dict and returns the namespace string.
_ADAPTER_NAMESPACE_RESOLVERS: dict[str, Any] = {
    "postgres": lambda p: f"postgres://{p.get('host', 'localhost')}:{p.get('port', 5432)}",
    "redshift": lambda p: f"redshift://{p.get('host', 'localhost')}:{p.get('port', 5439)}",
    "bigquery": lambda _: "bigquery",
    "snowflake": lambda p: f"snowflake://{p.get('account', '')}",
    "databricks": lambda p: f"databricks://{p.get('host', '')}",
    "spark": lambda p: f"spark://{p.get('host', 'localhost')}:{p.get('port', 10000)}",
    "trino": lambda p: f"trino://{p.get('host', 'localhost')}:{p.get('port', 8080)}",
    "mysql": lambda p: f"mysql://{p.get('host', 'localhost')}:{p.get('port', 3306)}",
    "oracle": lambda p: f"oracle://{p.get('host', 'localhost')}:{p.get('port', 1521)}",
    "clickhouse": lambda p: f"clickhouse://{p.get('host', 'localhost')}:{p.get('port', 8123)}",
    "vertica": lambda p: f"vertica://{p.get('host', 'localhost')}:{p.get('port', 5433)}",
    "exasol": lambda p: f"exasol://{p.get('host', 'localhost')}:{p.get('port', 8563)}",
    "athena": lambda p: f"athena://athena.{p.get('region_name', 'us-east-1')}.amazonaws.com",
    "duckdb": lambda p: f"duckdb://{p.get('path', '')}",
    "sqlserver": lambda p: f"sqlserver://{p.get('host', 'localhost')}:{p.get('port', 1433)}",
    "teradata": lambda p: f"teradata://{p.get('host', 'localhost')}",
    "starburst": lambda p: f"trino://{p.get('host', 'localhost')}:{p.get('port', 8080)}",
}


def get_dataset_alias_name(dag: DAG | None, task_group: TaskGroup | None, task_id: str) -> str:
    """
    Given the Airflow DAG, Airflow TaskGroup and the Airflow Task ID, return the name of the
    Airflow DatasetAlias associated to that task.
    """
    dag_id = None
    task_group_id = None

    if task_group:
        if task_group.dag_id is not None:
            dag_id = task_group.dag_id
        if task_group.group_id is not None:
            task_group_id = task_group.group_id
            task_group_id = task_group_id.split(".")[-1]
    elif dag:
        dag_id = dag.dag_id

    identifiers_list = []

    if dag_id:
        identifiers_list.append(dag_id)
    if task_group_id:
        identifiers_list.append(task_group_id)

    identifiers_list.append(task_id.split(".")[-1])

    return "__".join(identifiers_list)


def _get_profile_dict(profile_config: ProfileConfig) -> tuple[str, dict[str, Any]]:
    """
    Extract the adapter type and profile dict from a ProfileConfig.

    Supports both profile_mapping and profiles_yml_filepath modes.

    :returns: Tuple of (adapter_type, profile_dict)
    """
    if profile_config.profile_mapping:
        adapter_type = profile_config.profile_mapping.dbt_profile_type
        profile_dict = profile_config.profile_mapping.profile
        return adapter_type, profile_dict

    if profile_config.profiles_yml_filepath:
        with open(profile_config.profiles_yml_filepath) as f:
            profiles = yaml.safe_load(f)
        target = profiles[profile_config.profile_name]["outputs"][profile_config.target_name]
        adapter_type = target.get("type", "")
        return adapter_type, target

    return "", {}


def get_dataset_namespace(profile_config: ProfileConfig) -> str | None:
    """
    Derive an OpenLineage-compatible dataset namespace from a Cosmos ProfileConfig.

    The namespace is adapter-specific: e.g. ``postgres://host:5432``, ``bigquery``,
    ``snowflake://account``.

    Returns ``None`` if the namespace cannot be determined.
    """
    try:
        adapter_type, profile_dict = _get_profile_dict(profile_config)
    except Exception:
        logger.debug("Unable to extract profile info for dataset namespace derivation", exc_info=True)
        return None

    if not adapter_type:
        return None

    resolver = _ADAPTER_NAMESPACE_RESOLVERS.get(adapter_type)
    if resolver:
        return str(resolver(profile_dict))

    # Fallback for unknown adapters
    return f"{adapter_type}://"


def construct_dataset_uri(namespace: str, database: str, schema: str, alias: str) -> str:
    """
    Construct an OL-compatible Airflow Asset/Dataset URI from its components.

    Uses the same Airflow 2 vs 3 URI logic as ``DbtLocalBaseOperator._create_asset_uri``.
    """
    name = f"{database}.{schema}.{alias}"
    airflow_2_uri = namespace + "/" + urllib.parse.quote(name)
    airflow_3_uri = namespace + "/" + urllib.parse.quote(name).replace(".", "/")

    if AIRFLOW_VERSION < Version("3.0.0"):
        if settings.use_dataset_airflow3_uri_standard:
            return airflow_3_uri
        return airflow_2_uri

    return airflow_3_uri


def compute_model_outlet_uris(manifest_path: str | Path, namespace: str) -> dict[str, list[str]]:
    """
    Read a dbt manifest and compute per-model outlet URIs.

    :param manifest_path: Path to the ``manifest.json`` file.
    :param namespace: The OL-compatible dataset namespace (e.g. ``postgres://host:5432``).
    :returns: Mapping of ``{unique_id: [uri]}`` for model, seed, and snapshot nodes.
    """
    _RESOURCE_TYPES_WITH_DATASETS = {"model", "seed", "snapshot"}

    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.debug("Unable to read manifest at %s for dataset URI computation", manifest_path, exc_info=True)
        return {}

    result: dict[str, list[str]] = {}
    for unique_id, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type", "")
        if resource_type not in _RESOURCE_TYPES_WITH_DATASETS:
            continue

        database = node.get("database", "")
        schema = node.get("schema", "")
        alias = node.get("alias") or node.get("name", "")

        if not all([database, schema, alias]):
            continue

        uri = construct_dataset_uri(namespace, database, schema, alias)
        result[unique_id] = [uri]

    return result
