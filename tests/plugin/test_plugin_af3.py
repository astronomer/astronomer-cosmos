# This module verifies the Airflow 3.x plugin is available via FastAPI.
import pytest
from airflow import __version__ as airflow_version
from packaging import version

import cosmos.plugin


@pytest.mark.skipif(version.parse(airflow_version).major == 2, reason="Airflow 2 uses FAB plugin")
def test_cosmos_plugin_enabled_on_airflow3():
    # Ensure the Airflow 3 plugin class is exposed
    assert cosmos.plugin.CosmosPlugin is not None
