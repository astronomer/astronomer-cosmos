import tempfile
from pathlib import Path

from airflow.configuration import conf

from cosmos.constants import DEFAULT_COSMOS_CACHE_DIR_NAME

DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache = conf.get("cosmos", "enable_cache", fallback=True) or True
