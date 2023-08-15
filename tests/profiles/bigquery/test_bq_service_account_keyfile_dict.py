import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

sample_keyfile_dict = {
    "type": "service_account",
    "private_key_id": "my_private_key_id",
    "private_key": "my_private_key",
}


@pytest.fixture(params=[sample_keyfile_dict, json.dumps(sample_keyfile_dict)])
def mock_bigquery_conn_with_dict(request):  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {
        "project": "my_project",
        "dataset": "my_dataset",
        "keyfile_dict": request.param,
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
    mock_bigquery_conn_with_dict.extra = json.dumps({"project": "my_project", "keyfile_dict": sample_keyfile_dict})
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
        "keyfile_json": {
            "type": "service_account",
            "private_key_id": "{{ env_var('COSMOS_CONN_GOOGLE_CLOUD_PLATFORM_PRIVATE_KEY_ID') }}",
            "private_key": "{{ env_var('COSMOS_CONN_GOOGLE_CLOUD_PLATFORM_PRIVATE_KEY') }}",
        },
    }
    assert profile_mapping.profile == expected


def test_profile_env_vars(mock_bigquery_conn_with_dict: Connection):
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn_with_dict.conn_id,
        {"dataset": "my_dataset"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_GOOGLE_CLOUD_PLATFORM_PRIVATE_KEY_ID": "my_private_key_id",
        "COSMOS_CONN_GOOGLE_CLOUD_PLATFORM_PRIVATE_KEY": "my_private_key",
    }
