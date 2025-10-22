import json
from collections import namedtuple
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.exceptions import CosmosValueError
from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

ConnExtraParams = namedtuple("ConnExtraParams", ["keyfile_dict", "keyfile_json_extra_key"])
sample_keyfile_dict = {
    "type": "service_account",
    "private_key_id": "my_private_key_id",
    "private_key": "my_private_key",
}


def get_fixture_params():
    """
    Make a matrix of params for the fixture that mock connection, as there are multiple fields in
    the airflow param mapping for the "keyfile_json" in GoogleCloudServiceAccountDictProfileMapping
    """
    fixture_params = []
    for d in (sample_keyfile_dict, json.dumps(sample_keyfile_dict)):
        for key in GoogleCloudServiceAccountDictProfileMapping.airflow_param_mapping.get("keyfile_json"):
            if key.startswith("extra."):
                key = key.replace("extra.", "")
            fixture_params.append(ConnExtraParams(keyfile_dict=d, keyfile_json_extra_key=key))
    return fixture_params


@pytest.fixture(params=get_fixture_params())
def mock_bigquery_conn_with_dict(request):  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {
        "project": "my_project",
        "dataset": "my_dataset",
        request.param.keyfile_json_extra_key: request.param.keyfile_dict,
    }
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
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
    # Remove the `project` key, which is mandatory
    mock_bigquery_conn_with_dict.extra = json.dumps({"dataset": "my_dataset", "keyfile_dict": sample_keyfile_dict})
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


def test_mock_profile(mock_bigquery_conn_with_dict: Connection):
    """
    Tests profile mock for keyfile_json is None. Giving keyfile_json a value will crash dbt ls.
    """
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(mock_bigquery_conn_with_dict, {})
    expected = {
        "type": "bigquery",
        "method": "service-account-json",
        "project": "mock_value",
        "dataset": "mock_value",
        "threads": 1,
        "keyfile_json": None,
    }
    assert profile_mapping.mock_profile == expected


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


def test_transform_keyfile_json_missing_dict():
    """
    Tests that a cosmos error is raised if the keyfile_json cannot be loaded as a dict.
    """
    keyfile_json = '["value"]'
    expected_cosmos_error = "keyfile_json cannot be loaded as a dict."

    profile_mapping = GoogleCloudServiceAccountDictProfileMapping("", {})
    with pytest.raises(CosmosValueError, match=expected_cosmos_error):
        profile_mapping.transform_keyfile_json(keyfile_json)


@pytest.mark.parametrize("missing_secret_key", ["private_key_id", "private_key"])
def test_transform_keyfile_json_missing_secret_key(missing_secret_key: str):
    """
    Tests that a cosmos error is raised if the keyfile_json is missing a secret key.
    """
    keyfile_json = {k: v for k, v in sample_keyfile_dict.items() if k != missing_secret_key}
    expected_cosmos_error = f"Could not find a value in service account json field: {missing_secret_key}."

    profile_mapping = GoogleCloudServiceAccountDictProfileMapping("", {})

    with pytest.raises(CosmosValueError, match=expected_cosmos_error):
        profile_mapping.transform_keyfile_json(keyfile_json)
