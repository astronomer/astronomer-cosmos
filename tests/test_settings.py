import os
import warnings
from importlib import reload
from unittest.mock import patch

from cosmos import settings


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


@patch.dict(os.environ, {"AIRFLOW__COSMOS__WATCHER_DBT_EXECUTION_QUEUE": "legacy_queue"}, clear=True)
def test_watcher_dbt_execution_queue_is_backwards_compatible_fallback():
    """If only the legacy queue is set, then producer and retry settings should take that value."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        reload(settings)

    assert settings.watcher_dbt_execution_queue == "legacy_queue"
    assert settings.watcher_dbt_producer_queue == "legacy_queue"
    assert settings.watcher_dbt_retry_queue == "legacy_queue"
    assert settings.watcher_dbt_consumer_queue is None  # Explicitly not set
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


@patch.dict(
    os.environ,
    {
        "AIRFLOW__COSMOS__WATCHER_DBT_EXECUTION_QUEUE": "legacy_queue",
        "AIRFLOW__COSMOS__WATCHER_DBT_PRODUCER_QUEUE": "producer_queue",
        "AIRFLOW__COSMOS__WATCHER_DBT_RETRY_QUEUE": "retry_queue",
    },
    clear=True,
)
def test_watcher_dbt_execution_queue_does_not_override_explicit_new_settings():
    """Producer and retry queue take priority over legacy execution queue."""
    reload(settings)
    assert settings.watcher_dbt_producer_queue == "producer_queue"
    assert settings.watcher_dbt_retry_queue == "retry_queue"


@patch.dict(os.environ, {}, clear=True)
def test_watcher_dbt_execution_queue_defaults_to_none_without_warning():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        reload(settings)

    assert settings.watcher_dbt_execution_queue is None
    assert settings.watcher_dbt_producer_queue is None
    assert settings.watcher_dbt_retry_queue is None
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)
