"""Tests for the BigQuery profile."""

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.bigquery.oauth import (
    GoogleCloudOauthProfileMapping,
)


@pytest.fixture()
def mock_bigquery_conn(request):
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {"project": "my_project", "dataset": "my_dataset"} if not hasattr(request, "param") else request.param
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_bigquery_mapping_selected(mock_bigquery_conn: Connection):
    profile_mapping = get_automatic_profile_mapping(mock_bigquery_conn.conn_id, {})
    assert isinstance(profile_mapping, GoogleCloudOauthProfileMapping)


@pytest.mark.parametrize(
    "mock_bigquery_conn", [{"project": "my_project"}, {"dataset": "my_dataset"}, {}], indirect=True
)
def test_connection_claiming_fails(mock_bigquery_conn: Connection) -> None:
    """
    Tests that the BigQuery profile mapping claims the correct connection type.
    """
    profile_mapping = GoogleCloudOauthProfileMapping(mock_bigquery_conn)
    assert not profile_mapping.can_claim_connection()


def test_connection_claiming_succeeds(mock_bigquery_conn: Connection):
    profile_mapping = GoogleCloudOauthProfileMapping(mock_bigquery_conn, {})
    assert profile_mapping.can_claim_connection()


def test_profile(mock_bigquery_conn: Connection):
    profile_mapping = GoogleCloudOauthProfileMapping(mock_bigquery_conn, {})
    expected = {
        "type": "bigquery",
        "method": "oauth",
        "project": "my_project",
        "dataset": "my_dataset",
        "threads": 1,
    }
    assert profile_mapping.profile == expected
