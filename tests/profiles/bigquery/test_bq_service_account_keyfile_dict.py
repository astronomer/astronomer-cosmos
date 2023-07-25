import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping


@pytest.fixture()
def mock_bigquery_conn_with_dict():  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {
        "project": "my_project",
        "dataset": "my_dataset",
        "keyfile_dict": {"key": "value"},
    }
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_profile(mock_bigquery_conn_with_dict: Connection):
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(mock_bigquery_conn_with_dict, {})
    expected = {
        "type": "bigquery",
        "method": "service-account-json",
        "project": "my_project",
        "dataset": "my_dataset",
        "threads": 1,
        "keyfile_json": {"key": "value"},
    }
    assert profile_mapping.profile == expected
