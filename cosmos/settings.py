import os
import tempfile
from pathlib import Path

import airflow
from airflow.configuration import conf

from cosmos.constants import DEFAULT_COSMOS_CACHE_DIR_NAME, DEFAULT_OPENLINEAGE_NAMESPACE

# In MacOS users may want to set the envvar `TMPDIR` if they do not want the value of the temp directory to change
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache = conf.getboolean("cosmos", "enable_cache", fallback=True)
experimental_cache = conf.getboolean("cosmos", "experimental_cache", fallback=False)
propagate_logs = conf.getboolean("cosmos", "propagate_logs", fallback=True)
dbt_docs_dir = conf.get("cosmos", "dbt_docs_dir", fallback=None)
dbt_docs_conn_id = conf.get("cosmos", "dbt_docs_conn_id", fallback=None)
dbt_docs_index_file_name = conf.get("cosmos", "dbt_docs_index_file_name", fallback="index.html")

try:
    LINEAGE_NAMESPACE = conf.get("openlineage", "namespace")
except airflow.exceptions.AirflowConfigException:
    LINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_OPENLINEAGE_NAMESPACE)
