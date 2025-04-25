# This module is added specifically to test that the plugin is disabled in Airflow 3.x.
# Currently, we skip the entire test_plugin.py module since it contains multiple tests that are only compatible with Airflow 2.x.
# Adding this test to that file would require selectively applying pytest.mark.skipif to each individual test,
# which would add unnecessary complexity.
# To keep things straightforward, we isolate the one relevant test for Airflow 3 here,
# while continuing to skip the existing plugin test module entirely for Airflow 3.x.
import pytest
from airflow import __version__ as airflow_version
from packaging import version

import cosmos.plugin


@pytest.mark.skipif(version.parse(airflow_version).major == 2, reason="Plugin is available in Airflow 2.x")
def test_cosmos_plugin_disabled_on_airflow3():
    assert cosmos.plugin.CosmosPlugin is None
