from __future__ import annotations

import json
import os
import re
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from packaging.version import Version

from cosmos import settings
from cosmos.constants import _DATASET_EMITTING_RESOURCE_TYPES, AIRFLOW_VERSION
from cosmos.log import get_logger

if TYPE_CHECKING:
    from cosmos.config import ProfileConfig

    try:
        from airflow.sdk import DAG

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
    """Derive the AWS Glue namespace, matching OL's ARN logic.

    Returns an empty string when the account ID cannot be determined,
    which causes ``get_dataset_namespace`` to treat it as a failed resolution.
    """
    region = profile.get("region", "us-east-1")
    if "account_id" in profile and profile["account_id"]:
        return f"arn:aws:glue:{region}:{profile['account_id']}"
    role_arn = profile.get("role_arn")
    if isinstance(role_arn, str) and role_arn:
        parts = role_arn.split(":")
        if len(parts) > 4 and parts[4]:
            return f"arn:aws:glue:{region}:{parts[4]}"
    return ""


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


# Matches dbt's `{{ env_var('NAME') }}` / `{{ env_var('NAME', 'default') }}` Jinja calls only.
# Deliberately narrower than a general Jinja renderer: profiles.yml values are only ever
# substituted for this specific dbt construct, so no other Jinja syntax is evaluated.
_ENV_VAR_PATTERN = re.compile(
    r"""\{\{\s*env_var\(\s*(['"])(?P<name>.*?)\1\s*(?:,\s*(['"])(?P<default>.*?)\3\s*)?\)\s*\}\}"""
)


def _render_env_var(value: str) -> str:
    """Substitute dbt-style ``env_var('NAME'[, 'default'])`` Jinja calls with the environment value."""

    def _replace(match: re.Match[str]) -> str:
        return os.getenv(match.group("name"), match.group("default") or "")

    return _ENV_VAR_PATTERN.sub(_replace, value)


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
        # dbt renders env_var() Jinja in profiles.yml; replicate that since we read the file directly.
        target = {key: _render_env_var(value) if isinstance(value, str) else value for key, value in target.items()}
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
    except (AttributeError, KeyError, TypeError, OSError, yaml.YAMLError):
        logger.debug("Unable to extract profile info for dataset namespace derivation", exc_info=True)
        return None

    if not adapter_type:
        return None

    resolver = _ADAPTER_NAMESPACE_RESOLVERS.get(adapter_type)
    if resolver:
        namespace = str(resolver(profile_dict))
        # Reject empty or scheme-only namespaces (e.g. "snowflake://", "databricks://")
        # that result from missing required profile fields.
        if not namespace or namespace.endswith("://"):
            logger.debug(
                "Resolved namespace '%s' for adapter '%s' is incomplete; skipping dataset emission.",
                namespace,
                adapter_type,
            )
            return None
        return namespace

    # Unknown adapters: return None so dataset emission is skipped.
    # Only adapters supported by the OpenLineage dbt integration are in
    # _ADAPTER_NAMESPACE_RESOLVERS; emitting URIs for unsupported adapters
    # would produce URIs that don't match LOCAL/VIRTUALENV behavior.
    logger.debug(
        "Adapter type '%s' is not supported for dataset namespace resolution; skipping dataset emission.", adapter_type
    )
    return None


def construct_dataset_uri(namespace: str, name: str, warn_uri_migration: bool = True) -> str:
    """
    Construct an Airflow Asset/Dataset URI from an OL-compatible namespace and
    dataset name.

    On Airflow 2, dots in the name are preserved (``namespace/db.schema.table``).
    On Airflow 3 (or when ``use_dataset_airflow3_uri_standard`` is enabled),
    dots are replaced with slashes (``namespace/db/schema/table``).

    :param namespace: The OL-compatible dataset namespace (e.g. ``postgres://host:5432``).
    :param name: The dot-delimited dataset name (e.g. ``database.schema.table``).
    :param warn_uri_migration: Whether to emit the Airflow 3 URI migration warning for this URI.
        Callers that create multiple URIs in one task run should pass ``False`` after
        the first generated URI.
    """
    airflow_2_uri = namespace + "/" + urllib.parse.quote(name)
    airflow_3_uri = namespace + "/" + urllib.parse.quote(name).replace(".", "/")

    if AIRFLOW_VERSION < Version("3.0.0"):
        if settings.use_dataset_airflow3_uri_standard:
            return airflow_3_uri
        if warn_uri_migration:
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

    if warn_uri_migration and not settings.use_dataset_airflow3_uri_standard:
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


def make_dataset_uri_builder() -> Callable[[str, str], str]:
    """Return a :func:`construct_dataset_uri` wrapper that emits the Airflow 3 URI
    migration warning only for the first URI it builds.

    A single task run can generate many dataset URIs -- one per dbt
    model/seed/snapshot, plus OpenLineage inputs/outputs -- and emitting the
    migration warning for every one floods the logs (#2778). Callers that build
    URIs in bulk should route them all through one builder so the guidance is
    logged once instead of once per dataset.

    The "already warned" state lives in the returned closure rather than at
    module level, so each new builder (i.e. each task run) warns once again
    instead of being silenced permanently for the lifetime of the worker process.
    """
    warned = False

    def build(namespace: str, name: str) -> str:
        nonlocal warned
        uri = construct_dataset_uri(namespace, name, warn_uri_migration=not warned)
        warned = True
        return uri

    return build


def compute_model_outlet_uris(manifest_path: str | Path, namespace: str) -> dict[str, list[str]]:
    """
    Read a dbt manifest and compute per-model outlet URIs.

    :param manifest_path: Path to the ``manifest.json`` file.
    :param namespace: The OL-compatible dataset namespace (e.g. ``postgres://host:5432``).
    :returns: Mapping of ``{unique_id: [uri]}`` for model, seed, and snapshot nodes.
    """
    # The manifest may not exist if dbt failed before completing compilation,
    # or if the temp project directory was cleaned up before this function ran.
    # In those cases we gracefully return an empty dict — consumers simply
    # won't emit datasets for the affected run.
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError):
        logger.warning(
            "Unable to read manifest at %s for dataset URI computation. Dataset emission will be skipped.",
            manifest_path,
            exc_info=True,
        )
        return {}

    result: dict[str, list[str]] = {}
    build_uri = make_dataset_uri_builder()
    for unique_id, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type", "")
        if resource_type not in _DATASET_EMITTING_RESOURCE_TYPES:
            continue

        database = node.get("database", "")
        schema = node.get("schema", "")
        alias = node.get("alias") or node.get("name", "")

        if not all([database, schema, alias]):
            continue

        result[unique_id] = [build_uri(namespace, f"{database}.{schema}.{alias}")]

    return result
