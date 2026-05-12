import os
from importlib import reload
from unittest.mock import patch

from cosmos import settings


@patch.dict(os.environ, {"AIRFLOW__COSMOS__ENABLE_CACHE": "False"}, clear=True)
def test_enable_cache_env_var():
    reload(settings)
    assert settings.enable_cache is False


@patch.dict(os.environ, {"AIRFLOW__COSMOS__ENABLE_DEBUG_MODE": "True"}, clear=True)
def test_enable_debug_mode_env_var():
    reload(settings)
    assert settings.enable_debug_mode is True


@patch.dict(
    os.environ,
    {"AIRFLOW__COSMOS__DEBUG_MEMORY_POLL_INTERVAL_SECONDS": "0.25"},
    clear=True,
)
def test_debug_memory_poll_interval_env_var():
    reload(settings)
    assert settings.debug_memory_poll_interval_seconds == 0.25
