import os
from importlib import reload
from unittest.mock import patch

import pytest

from cosmos import settings


@pytest.fixture(autouse=True)
def _restore_settings_after_env_var_test():
    # Tests here reload the settings module under a patched environment; without a
    # final reload the patched values leak into later test modules
    yield
    reload(settings)


@patch.dict(os.environ, {"AIRFLOW__COSMOS__ENABLE_CACHE": "False"}, clear=True)
def test_enable_cache_env_var():
    reload(settings)
    assert settings.enable_cache is False


@patch.dict(
    os.environ,
    {"AIRFLOW__COSMOS__ENABLE_HIERARCHICAL_NAMING_FOR_GROUP_NODES_BY_FOLDER": "True"},
    clear=True,
)
def test_enable_hierarchical_naming_for_group_nodes_by_folder_env_var():
    reload(settings)
    assert settings.enable_hierarchical_naming_for_group_nodes_by_folder is True


@patch.dict(os.environ, {}, clear=True)
def test_enable_hierarchical_naming_for_group_nodes_by_folder_defaults_to_false():
    reload(settings)
    assert settings.enable_hierarchical_naming_for_group_nodes_by_folder is False


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


@patch.dict(os.environ, {}, clear=True)
def test_project_hash_excluded_dirs_defaults_to_generated_dirs():
    reload(settings)
    assert settings.project_hash_excluded_dirs == frozenset({".git", "target", "dbt_packages", "logs"})


@patch.dict(
    os.environ,
    {"AIRFLOW__COSMOS__PROJECT_HASH_EXCLUDED_DIRS": "target, dbt_packages , scratch"},
    clear=True,
)
def test_project_hash_excluded_dirs_env_var_replaces_default():
    reload(settings)
    assert settings.project_hash_excluded_dirs == frozenset({"target", "dbt_packages", "scratch"})
