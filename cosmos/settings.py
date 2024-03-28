import tempfile
from pathlib import Path

from airflow.configuration import conf

from cosmos.constants import DEFAULT_COSMOS_CACHE_DIR_NAME

default_cache_dir = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=default_cache_dir) or default_cache_dir)
