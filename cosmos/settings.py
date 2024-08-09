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
    FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP,
)
from cosmos.exceptions import CosmosValueError

# In MacOS users may want to set the envvar `TMPDIR` if they do not want the value of the temp directory to change
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache = conf.getboolean("cosmos", "enable_cache", fallback=True)
enable_cache_partial_parse = conf.getboolean("cosmos", "enable_cache_partial_parse", fallback=True)
enable_cache_package_lockfile = conf.getboolean("cosmos", "enable_cache_package_lockfile", fallback=True)
enable_cache_dbt_ls = conf.getboolean("cosmos", "enable_cache_dbt_ls", fallback=True)
propagate_logs = conf.getboolean("cosmos", "propagate_logs", fallback=True)
dbt_docs_dir = conf.get("cosmos", "dbt_docs_dir", fallback=None)
dbt_docs_conn_id = conf.get("cosmos", "dbt_docs_conn_id", fallback=None)
dbt_docs_index_file_name = conf.get("cosmos", "dbt_docs_index_file_name", fallback="index.html")
enable_cache_profile = conf.getboolean("cosmos", "enable_cache_profile", fallback=True)
dbt_profile_cache_dir_name = conf.get("cosmos", "profile_cache_dir_name", fallback="profile")

try:
    LINEAGE_NAMESPACE = conf.get("openlineage", "namespace")
except airflow.exceptions.AirflowConfigException:
    LINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_OPENLINEAGE_NAMESPACE)

AIRFLOW_IO_AVAILABLE = Version(airflow_version) >= Version("2.8.0")


def _configure_remote_cache_path() -> Path | None:
    remote_cache_path_str = str(conf.get("cosmos", "remote_cache_path", fallback=""))
    remote_cache_conn_id = str(conf.get("cosmos", "remote_cache_conn_id", fallback=""))
    cache_path = None

    if remote_cache_path_str and not AIRFLOW_IO_AVAILABLE:
        raise CosmosValueError(
            f"You're trying to specify dbt_ls_cache_remote_path {remote_cache_path_str}, but the required Object "
            f"Storage feature is unavailable in Airflow version {airflow_version}. Please upgrade to "
            f"Airflow 2.8 or later."
        )
    elif remote_cache_path_str:
        from airflow.io.path import ObjectStoragePath

        if not remote_cache_conn_id:
            remote_cache_conn_id = FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP.get(
                remote_cache_path_str.split("://")[0], None
            )

        cache_path = ObjectStoragePath(remote_cache_path_str, conn_id=remote_cache_conn_id)
        if not cache_path.exists():  # type: ignore[no-untyped-call]
            raise CosmosValueError(
                f"`remote_cache_path` {remote_cache_path_str} does not exist or is not accessible using "
                f"`remote_cache_conn_id` {remote_cache_conn_id}"
            )
    return cache_path


remote_cache_path = _configure_remote_cache_path()
