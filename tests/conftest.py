import json
from unittest.mock import patch

import pytest
from airflow.models import Connection


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

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn
