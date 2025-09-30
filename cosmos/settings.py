from __future__ import annotations

import os
import tempfile
from pathlib import Path

import airflow
from airflow.configuration import conf
from airflow.version import version as airflow_version
from packaging.version import Version

from cosmos.constants import (
    DEFAULT_COSMOS_CACHE_DIR_NAME,
    DEFAULT_OPENLINEAGE_NAMESPACE,
)

# In MacOS users may want to set the envvar `TMPDIR` if they do not want the value of the temp directory to change
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache = conf.getboolean("cosmos", "enable_cache", fallback=True)
enable_dataset_alias = conf.getboolean("cosmos", "enable_dataset_alias", fallback=True)
use_dataset_airflow3_uri_standard = conf.getboolean(
    "cosmos",
    "enable_dataset_airflow3_uri",
    fallback=conf.getboolean("cosmos", "use_dataset_airflow3_uri_standard", fallback=False),
)
enable_cache_partial_parse = conf.getboolean("cosmos", "enable_cache_partial_parse", fallback=True)
enable_cache_package_lockfile = conf.getboolean("cosmos", "enable_cache_package_lockfile", fallback=True)
enable_cache_dbt_ls = conf.getboolean("cosmos", "enable_cache_dbt_ls", fallback=True)
rich_logging = conf.getboolean("cosmos", "rich_logging", fallback=False)
dbt_docs_dir = conf.get("cosmos", "dbt_docs_dir", fallback=None)
dbt_docs_conn_id = conf.get("cosmos", "dbt_docs_conn_id", fallback=None)
dbt_docs_index_file_name = conf.get("cosmos", "dbt_docs_index_file_name", fallback="index.html")
enable_cache_profile = conf.getboolean("cosmos", "enable_cache_profile", fallback=True)
dbt_profile_cache_dir_name = conf.get("cosmos", "profile_cache_dir_name", fallback="profile")
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

AIRFLOW_IO_AVAILABLE = Version(airflow_version) >= Version("2.8.0")

# The following environment variable is populated in Astro Cloud
in_astro_cloud = os.getenv("ASTRONOMER_ENVIRONMENT") == "cloud"

try:
    LINEAGE_NAMESPACE = conf.get("openlineage", "namespace")
except airflow.exceptions.AirflowConfigException:
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
