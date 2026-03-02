import os
import subprocess
import textwrap
from importlib import reload
from unittest.mock import patch

from cosmos import settings


@patch.dict(os.environ, {"AIRFLOW__COSMOS__ENABLE_CACHE": "False"}, clear=True)
def test_enable_cache_env_var():
    reload(settings)
    assert settings.enable_cache is False


def test_enable_memory_optimised_imports_true(monkeypatch):
    script = textwrap.dedent("""
            import os
            os.environ["AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS"] = "True"
            import cosmos
            assert cosmos.settings.enable_memory_optimised_imports is True
            assert not hasattr(cosmos, "DbtDag")
        """)

    result = subprocess.run(["python", "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_enable_memory_optimised_imports_false(monkeypatch):
    script = textwrap.dedent("""
            import os
            os.environ["AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS"] = "False"
            import cosmos
            assert cosmos.settings.enable_memory_optimised_imports is False
            assert hasattr(cosmos, "DbtDag")
        """)

    result = subprocess.run(["python", "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


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
