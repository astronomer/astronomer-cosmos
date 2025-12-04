"""Tests for the databricks OAuth profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.databricks import DatabricksOauthProfileMapping


@pytest.fixture()
def mock_databricks_conn():  # type: ignore
    """
    Mocks and returns an Airflow Databricks connection.
    """
    conn = Connection(
        conn_id="my_databricks_connection",
        conn_type="databricks",
        host="https://my_host",
        login="my_client_id",
        password="my_client_secret",
        extra='{"http_path": "my_http_path"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Databricks profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == databricks
    # and the following exist:
    # - schema
    # - host
    # - http_path
    # - client_id
    # - client_secret
    potential_values = {
        "conn_type": "databricks",
        "host": "my_host",
        "login": "my_client_id",
        "password": "my_client_secret",
        "extra": '{"http_path": "my_http_path"}',
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = DatabricksOauthProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksOauthProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksOauthProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_mock_profile() -> None:
    """
    Tests that the Databricks mock profile is generated correctly.
    """
    profile_mapping = DatabricksOauthProfileMapping("conn_id", {"schema": "my_schema"})
    assert profile_mapping.mock_profile.get("auth_type") == "oauth"
