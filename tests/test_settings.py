import importlib
import os
import sys
from importlib import reload
from unittest.mock import patch

from cosmos import settings


@patch.dict(os.environ, {"AIRFLOW__COSMOS__ENABLE_CACHE": "False"}, clear=True)
def test_enable_cache_env_var():
    reload(settings)
    assert settings.enable_cache is False


def test_explicit_imports_true(monkeypatch):
    monkeypatch.setenv("AIRFLOW__COSMOS__EXPLICIT_IMPORTS", "True")
    importlib.invalidate_caches()
    if "cosmos.settings" in sys.modules:
        importlib.reload(sys.modules["cosmos.settings"])
    if "cosmos" in sys.modules:
        del sys.modules["cosmos"]
    import cosmos

    assert cosmos.settings.explicit_imports is True
    # DbtDag should not be imported at top-level
    assert not hasattr(cosmos, "DbtDag")


def test_explicit_imports_false(monkeypatch):
    monkeypatch.setenv("AIRFLOW__COSMOS__EXPLICIT_IMPORTS", "False")
    importlib.invalidate_caches()
    if "cosmos.settings" in sys.modules:
        importlib.reload(sys.modules["cosmos.settings"])
    if "cosmos" in sys.modules:
        del sys.modules["cosmos"]
    import cosmos

    assert cosmos.settings.explicit_imports is False
    # DbtDag should be imported at top-level
    assert hasattr(cosmos, "DbtDag")
