from __future__ import annotations

import os
import tempfile
import warnings
from pathlib import Path

import airflow

try:
    from airflow.providers.common.compat.sdk import conf
except ImportError:
    from airflow.configuration import conf

from cosmos.constants import (
    DEFAULT_COSMOS_CACHE_DIR_NAME,
    DEFAULT_OPENLINEAGE_NAMESPACE,
)

# In MacOS users may want to set the envvar `TMPDIR` if they do not want the value of the temp directory to change
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir: Path = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache: bool = conf.getboolean("cosmos", "enable_cache", fallback=True)
enable_dag_versioning = conf.getboolean("cosmos", "enable_dag_versioning", fallback=True)
enable_dataset_alias = conf.getboolean("cosmos", "enable_dataset_alias", fallback=True)
enable_uri_xcom = conf.getboolean("cosmos", "enable_uri_xcom", fallback=False)
use_dataset_airflow3_uri_standard = conf.getboolean(
    "cosmos",
    "enable_dataset_airflow3_uri",
    fallback=conf.getboolean("cosmos", "use_dataset_airflow3_uri_standard", fallback=False),
)
enable_cache_partial_parse = conf.getboolean("cosmos", "enable_cache_partial_parse", fallback=True)
enable_cache_package_lockfile = conf.getboolean("cosmos", "enable_cache_package_lockfile", fallback=True)
enable_cache_dbt_ls = conf.getboolean("cosmos", "enable_cache_dbt_ls", fallback=True)
enable_cache_dbt_yaml_selectors = conf.getboolean("cosmos", "enable_cache_dbt_yaml_selectors", fallback=True)
enable_lax_selector_parsing = conf.getboolean("cosmos", "enable_lax_selector_parsing", fallback=False)
# dbt-core discovers plugins by importing every top-level ``dbt_*`` module on ``sys.path``/``PYTHONPATH``.
# Airflow puts its DAGs folder there, so a DAG file named ``dbt_*.py`` gets imported as a side effect of
# Cosmos running dbt (see #1673). Enabled by default, Cosmos strips the DAGs folder from dbt's import path
# for the duration of each dbt invocation. Disabling this restores the previous behaviour -- useful if a
# genuine dbt plugin lives inside the DAGs folder and must stay discoverable.
enable_dags_folder_exclusion_from_dbt = conf.getboolean(
    "cosmos", "enable_dags_folder_exclusion_from_dbt", fallback=True
)
# When RenderConfig.group_nodes_by_folder is enabled, key folder task groups by their full path so
# that folders sharing a leaf name under different parents render as distinct task groups. Defaults
# to False to preserve existing task-group ids (enabling it is a breaking change for DAGs that
# reference Cosmos task groups by id); expected to become the default in Cosmos 2.0. See #2824.
enable_hierarchical_naming_for_group_nodes_by_folder = conf.getboolean(
    "cosmos", "enable_hierarchical_naming_for_group_nodes_by_folder", fallback=False
)
rich_logging = conf.getboolean("cosmos", "rich_logging", fallback=False)
dbt_docs_dir = conf.get("cosmos", "dbt_docs_dir", fallback=None)
dbt_docs_conn_id = conf.get("cosmos", "dbt_docs_conn_id", fallback=None)
dbt_docs_index_file_name = conf.get("cosmos", "dbt_docs_index_file_name", fallback="index.html")
enable_cache_profile: bool = conf.getboolean("cosmos", "enable_cache_profile", fallback=True)
dbt_profile_cache_dir_name: str = conf.get("cosmos", "profile_cache_dir_name", fallback="profile")
virtualenv_max_retries_lock = conf.getint("cosmos", "virtualenv_max_retries_lock", fallback=120)
default_copy_dbt_packages = conf.getboolean("cosmos", "default_copy_dbt_packages", fallback=False)
pre_dbt_fusion = conf.getboolean("cosmos", "pre_dbt_fusion", fallback=False)

# Experimentally adding `remote_cache_dir` as a separate entity in the Cosmos 1.6 release to gather feedback.
# This will be merged with the `cache_dir` config parameter in upcoming releases.
remote_cache_dir = conf.get("cosmos", "remote_cache_dir", fallback=None)
remote_cache_dir_conn_id = conf.get("cosmos", "remote_cache_dir_conn_id", fallback=None)
remote_target_path = conf.get("cosmos", "remote_target_path", fallback=None)
remote_target_path_conn_id = conf.get("cosmos", "remote_target_path_conn_id", fallback=None)
upload_sql_to_xcom = conf.getboolean("cosmos", "upload_sql_to_xcom", fallback=True)

# Eager imports in cosmos/__init__.py expose all Cosmos classes at the top level,
# which can significantly increase memory usage—even when Cosmos is installed but not actively used.
# This option allows disabling those eager imports to reduce memory footprint.
# When enabled, users must access Cosmos classes via their full module paths,
# avoiding the overhead of importing unused modules and classes.
enable_memory_optimised_imports = conf.getboolean("cosmos", "enable_memory_optimised_imports", fallback=False)

# Related to async operators
enable_setup_async_task = conf.getboolean("cosmos", "enable_setup_async_task", fallback=True)
enable_teardown_async_task = conf.getboolean("cosmos", "enable_teardown_async_task", fallback=True)

# Deprecated: previously used for both the producer task and consumer retries. Kept for backwards
# compatibility as a fallback for watcher_dbt_producer_queue and watcher_dbt_retry_queue below.
watcher_dbt_execution_queue: str | None = conf.get("cosmos", "watcher_dbt_execution_queue", fallback=None)
if watcher_dbt_execution_queue:
    warnings.warn(
        "The `watcher_dbt_execution_queue` config is deprecated since Cosmos 1.16.0 and will be removed in "
        "Cosmos 2.0.0. Use `watcher_dbt_producer_queue` and/or `watcher_dbt_retry_queue` instead.",
        DeprecationWarning,
    )

# Separate queue configuration for each watcher task type:
# - watcher_dbt_producer_queue: queue for the DbtProducerWatcherOperator task
# - watcher_dbt_consumer_queue: queue for DbtConsumerWatcherSensor tasks on their initial run
# - watcher_dbt_retry_queue: queue for DbtConsumerWatcherSensor tasks on retries (typically needs more resources)
# watcher_dbt_producer_queue and watcher_dbt_retry_queue fall back to the deprecated
# watcher_dbt_execution_queue if set, preserving pre-1.16.0 behavior for existing users.
watcher_dbt_producer_queue: str | None = (
    conf.get("cosmos", "watcher_dbt_producer_queue", fallback=None) or watcher_dbt_execution_queue
)
watcher_dbt_consumer_queue: str | None = conf.get("cosmos", "watcher_dbt_consumer_queue", fallback=None)
watcher_dbt_retry_queue: str | None = (
    conf.get("cosmos", "watcher_dbt_retry_queue", fallback=None) or watcher_dbt_execution_queue
)

enable_watcher_reliable_retry = conf.getboolean("cosmos", "enable_watcher_reliable_retry", fallback=True)

# The following environment variable is populated in Astro Cloud
in_astro_cloud = os.getenv("ASTRONOMER_ENVIRONMENT") == "cloud"

try:
    from airflow.sdk._shared.configuration.exceptions import AirflowConfigException as SdkConfigException
except ImportError:
    SdkConfigException = airflow.exceptions.AirflowConfigException

try:
    LINEAGE_NAMESPACE = conf.get("openlineage", "namespace")
except SdkConfigException:
    LINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_OPENLINEAGE_NAMESPACE)


def convert_to_boolean(value: str | None) -> bool:
    """
    Convert a string that represents a boolean to a Python boolean.
    """
    value = str(value).lower().strip()
    if value in ("f", "false", "0", "", "none"):
        return False
    return True


# Telemetry-related settings
enable_telemetry = conf.getboolean("cosmos", "enable_telemetry", fallback=True)
do_not_track = convert_to_boolean(os.getenv("DO_NOT_TRACK"))
no_analytics = convert_to_boolean(os.getenv("SCARF_NO_ANALYTICS"))

# Debug mode - when enabled, Cosmos will track and push memory utilization to XCom
enable_debug_mode = conf.getboolean("cosmos", "enable_debug_mode", fallback=False)
debug_memory_poll_interval_seconds = conf.getfloat("cosmos", "debug_memory_poll_interval_seconds", fallback=0.5)

# Experimental: use orjson for faster dbt manifest.json parsing (disabled by default)
enable_orjson_parser = conf.getboolean("cosmos", "enable_orjson_parser", fallback=False)
