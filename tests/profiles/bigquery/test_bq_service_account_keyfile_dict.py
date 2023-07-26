import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
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


def test_bigquery_mapping_selected(mock_bigquery_conn_with_dict: Connection):
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn_with_dict.conn_id,
        {"dataset": "my_dataset"},
    )
    assert isinstance(profile_mapping, GoogleCloudServiceAccountDictProfileMapping)


def test_connection_claiming_succeeds(mock_bigquery_conn_with_dict: Connection):
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(mock_bigquery_conn_with_dict, {})
    assert profile_mapping.can_claim_connection()


def test_connection_claiming_fails(mock_bigquery_conn_with_dict: Connection):
    # Remove the `dataset` key, which is mandatory
    mock_bigquery_conn_with_dict.extra = json.dumps({"project": "my_project", "keyfile_dict": {"key": "value"}})
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(mock_bigquery_conn_with_dict, {})
    assert not profile_mapping.can_claim_connection()


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
