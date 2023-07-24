"Tests for the BigQuery profile."

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.bigquery.service_account_file import (
    GoogleCloudServiceAccountFileProfileMapping,
)


@pytest.fixture()
def mock_bigquery_conn():  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {
        "project": "my_project",
        "key_path": "my_key_path.json",
    }
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_profile_args(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
        },
    )

    assert profile_mapping.profile == {
        "type": "bigquery",
        "method": "service-account",
        "project": mock_bigquery_conn.extra_dejson.get("project"),
        "keyfile": mock_bigquery_conn.extra_dejson.get("key_path"),
        "dataset": "my_dataset",
        "threads": 1,
    }


def test_profile_args_overrides(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
            "threads": 2,
            "project": "my_project_override",
            "keyfile": "my_keyfile_override",
        },
    )

    assert profile_mapping.profile == {
        "type": "bigquery",
        "method": "service-account",
        "project": "my_project_override",
        "keyfile": "my_keyfile_override",
        "dataset": "my_dataset",
        "threads": 2,
    }


def test_profile_env_vars(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
        mock_bigquery_conn.conn_id,
        profile_args={"dataset": "my_dataset"},
    )
    assert profile_mapping.env_vars == {}
