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


def _resolve_snowflake_account(account: str) -> str:
    """Normalize a Snowflake account name, delegating to the OL helper when available."""
    try:
        from openlineage.common.provider.snowflake import fix_account_name

        return str(fix_account_name(account))
    except ImportError:
        return account


def _resolve_spark_namespace(profile: dict[str, Any]) -> str:
    """Derive the Spark namespace, matching OL's port-by-method logic."""
    host = profile.get("host", "localhost")
    if "port" in profile:
        port = profile["port"]
    elif profile.get("method") in ("http", "odbc"):
        port = 443
    elif profile.get("method") == "thrift":
        port = 10001
    else:
        port = 10000
    return f"spark://{host}:{port}"


def _resolve_glue_namespace(profile: dict[str, Any]) -> str:
    """Derive the AWS Glue namespace, matching OL's ARN logic."""
    region = profile.get("region", "us-east-1")
    if "account_id" in profile:
        return f"arn:aws:glue:{region}:{profile['account_id']}"
    role_arn = profile.get("role_arn", "")
    parts = role_arn.split(":")
    account_id = parts[4] if len(parts) > 4 else ""
    return f"arn:aws:glue:{region}:{account_id}"


# Mapping of dbt adapter type to a callable that derives the OL-compatible dataset namespace
# from the profile dict. Each callable receives the profile dict and returns the namespace string.
#
# Only adapters supported by the OpenLineage dbt integration are included so that URIs produced
# by WATCHER mode match those from LOCAL mode (which delegates to the OL library).
#
# Source: https://openlineage.io/docs/spec/naming/
# Reference impl: openlineage.common.provider.dbt.processor.DbtArtifactProcessor.extract_namespace
_ADAPTER_NAMESPACE_RESOLVERS: dict[str, Any] = {
    "postgres": lambda p: f"postgres://{p.get('host', 'localhost')}:{p.get('port', 5432)}",
    "redshift": lambda p: f"redshift://{p.get('host', 'localhost')}:{p.get('port', 5439)}",
    "bigquery": lambda _: "bigquery",
    "snowflake": lambda p: f"snowflake://{_resolve_snowflake_account(p.get('account', ''))}",
    "databricks": lambda p: f"databricks://{p.get('host', '')}",
    "spark": _resolve_spark_namespace,
    "trino": lambda p: f"trino://{p.get('host', 'localhost')}:{p.get('port', 8080)}",
    "clickhouse": lambda p: f"clickhouse://{p.get('host', 'localhost')}:{p.get('port', 8123)}",
    "sqlserver": lambda p: f"mssql://{p.get('server', 'localhost')}:{p.get('port', 1433)}",
    "dremio": lambda p: f"dremio://{p.get('software_host', 'localhost')}:{p.get('port', 443)}",
    "athena": lambda p: f"awsathena://athena.{p.get('region_name', 'us-east-1')}.amazonaws.com",
    "glue": _resolve_glue_namespace,
    "duckdb": lambda p: f"duckdb://{p.get('path', '')}",
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

    If the adapter type is recognised in ``_ADAPTER_NAMESPACE_RESOLVERS``, the
    corresponding resolver builds the namespace from connection details in the
    profile dict.

    Returns ``None`` if the namespace cannot be determined (e.g. missing or
    invalid profile configuration, or an unsupported adapter type). When
    ``None`` is returned, dataset emission is skipped for the run.
    """
    try:
        adapter_type, profile_dict = _get_profile_dict(profile_config)
    except (AttributeError, KeyError, TypeError, FileNotFoundError, yaml.YAMLError):
        logger.debug("Unable to extract profile info for dataset namespace derivation", exc_info=True)
        return None

    if not adapter_type:
        return None

    resolver = _ADAPTER_NAMESPACE_RESOLVERS.get(adapter_type)
    if resolver:
        return str(resolver(profile_dict))

    # Unknown adapters: return None so dataset emission is skipped.
    # Only adapters supported by the OpenLineage dbt integration are in
    # _ADAPTER_NAMESPACE_RESOLVERS; emitting URIs for unsupported adapters
    # would produce URIs that don't match LOCAL/VIRTUALENV behavior.
    logger.debug(
        "Adapter type '%s' is not supported for dataset namespace resolution; skipping dataset emission.", adapter_type
    )
    return None


def construct_dataset_uri(namespace: str, name: str) -> str:
    """
    Construct an Airflow Asset/Dataset URI from an OL-compatible namespace and
    dataset name.

    On Airflow 2, dots in the name are preserved (``namespace/db.schema.table``).
    On Airflow 3 (or when ``use_dataset_airflow3_uri_standard`` is enabled),
    dots are replaced with slashes (``namespace/db/schema/table``).

    :param namespace: The OL-compatible dataset namespace (e.g. ``postgres://host:5432``).
    :param name: The dot-delimited dataset name (e.g. ``database.schema.table``).
    """
    airflow_2_uri = namespace + "/" + urllib.parse.quote(name)
    airflow_3_uri = namespace + "/" + urllib.parse.quote(name).replace(".", "/")

    if AIRFLOW_VERSION < Version("3.0.0"):
        if settings.use_dataset_airflow3_uri_standard:
            return airflow_3_uri
        logger.warning(
            "Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs "
            "(standard used by Cosmos) will no longer be valid. "
            "Therefore, if using Cosmos with Airflow 3, the Airflow Dataset URIs will be changed to <%s>. "
            "Previously, with Airflow 2.x, the URI was <%s>. "
            "If you want to use the Airflow 3 URI standard while still using Airflow 2, please set: "
            "export AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1 "
            "Remember to update any DAGs that are scheduled using this dataset.",
            airflow_3_uri,
            airflow_2_uri,
        )
        return airflow_2_uri

    logger.warning(
        "Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs "
        "(standard used by Cosmos) are no longer accepted. "
        "Therefore, if using Cosmos with Airflow 3, the Airflow Asset (Dataset) URI is now <%s>. "
        "Before, with Airflow 2.x, the URI used to be <%s>. "
        "Please, change any DAGs that were scheduled using the old standard to the new one.",
        airflow_3_uri,
        airflow_2_uri,
    )
    return airflow_3_uri


def compute_model_outlet_uris(manifest_path: str | Path, namespace: str) -> dict[str, list[str]]:
    """
    Read a dbt manifest and compute per-model outlet URIs.

    :param manifest_path: Path to the ``manifest.json`` file.
    :param namespace: The OL-compatible dataset namespace (e.g. ``postgres://host:5432``).
    :returns: Mapping of ``{unique_id: [uri]}`` for model, seed, and snapshot nodes.
    """
    _RESOURCE_TYPES_WITH_DATASETS = {"model", "seed", "snapshot"}

    # The manifest may not exist if dbt failed before completing compilation,
    # or if the temp project directory was cleaned up before this function ran.
    # In those cases we gracefully return an empty dict — consumers simply
    # won't emit datasets for the affected run.
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning(
            "Unable to read manifest at %s for dataset URI computation. Dataset emission will be skipped.",
            manifest_path,
            exc_info=True,
        )
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

        uri = construct_dataset_uri(namespace, f"{database}.{schema}.{alias}")
        result[unique_id] = [uri]

    return result
