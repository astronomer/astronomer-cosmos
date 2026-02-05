import json
import os
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION

# Disable telemetry during tests to avoid network overhead from HTTP calls.
# Each telemetry call has a 1s timeout and fires on every task success/failure,
# which can add significant overhead (20+ seconds for DAGs with many tasks).
# Set this BEFORE importing cosmos.settings to ensure it takes effect.
os.environ["DO_NOT_TRACK"] = "1"

if AIRFLOW_VERSION >= Version("3.1"):
    # Change introduced in Airflow 3.1.0
    # https://github.com/apache/airflow/pull/55722/files
    base_operator_get_connection_path = "airflow.sdk.BaseHook.get_connection"
else:
    base_operator_get_connection_path = "airflow.hooks.base.BaseHook.get_connection"


@pytest.fixture(scope="session", autouse=True)
def disable_telemetry_for_tests():
    """
    Disable telemetry for all tests to avoid network overhead.

    The telemetry listeners make HTTP calls on every task success/failure,
    each with a 1s timeout. For DAGs with many tasks, this can overhead per test.
    """
    import cosmos.settings

    original_value = cosmos.settings.do_not_track
    cosmos.settings.do_not_track = True
    yield
    cosmos.settings.do_not_track = original_value


@pytest.fixture()
def mock_bigquery_conn():  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {"project": "my_project", "key_path": "my_key_path.json", "dataset": "test"}
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch(base_operator_get_connection_path, return_value=conn):
        yield conn
