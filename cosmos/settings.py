import tempfile
from pathlib import Path

from airflow.configuration import conf

from cosmos.constants import DEFAULT_COSMOS_CACHE_DIR_NAME

# In MacOS users may want to set the envvar `TMPDIR` if they do not want the value of the temp directory to change
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir(), DEFAULT_COSMOS_CACHE_DIR_NAME)
cache_dir = Path(conf.get("cosmos", "cache_dir", fallback=DEFAULT_CACHE_DIR) or DEFAULT_CACHE_DIR)
enable_cache = conf.get("cosmos", "enable_cache", fallback=True)
