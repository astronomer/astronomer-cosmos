"""Tests for the clickhouse profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.clickhouse.user_pass import (
    ClickhouseUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_clickhouse_conn():  # type: ignore
    """Sets the connection as an environment variable."""
    conn = Connection(
        conn_id="clickhouse_connection",
        conn_type="generic",
        host="my_host",
        login="my_user",
        password="my_password",
        schema="my_database",
        extra='{"clickhouse": "True"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming1() -> None:
    """
    Tests that the clickhouse profile mapping claims the correct connection type.

    should only claim when:
    - conn_type == generic
    And the following exist:
    - host
    - login
    - password
    - schema
    - extra.clickhouse
    """
    required_values = {
        "conn_type": "generic",
        "host": "my_host",
        "login": "my_user",
        "schema": "my_database",
        "extra": '{"clickhouse": "True"}',
    }

    def can_claim_with_missing_key(missing_key: str) -> bool:
        values = required_values.copy()
        del values[missing_key]
        conn = Connection(**values)  # type: ignore
        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = ClickhouseUserPasswordProfileMapping(conn, {})
            return profile_mapping.can_claim_connection()

    # if we're missing any of the required values, it shouldn't claim
    for key in required_values:
        assert not can_claim_with_missing_key(key), f"Failed when missing {key}"

    # if we have all the required values, it should claim
    conn = Connection(**required_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ClickhouseUserPasswordProfileMapping(conn, {})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_clickhouse_conn: Connection,
) -> None:
    """Tests that the correct profile mapping is selected."""
    profile_mapping = get_automatic_profile_mapping(mock_clickhouse_conn.conn_id, {})
    assert isinstance(profile_mapping, ClickhouseUserPasswordProfileMapping)


def test_profile_args(mock_clickhouse_conn: Connection) -> None:
    """Tests that the profile values get set correctly."""
    profile_mapping = get_automatic_profile_mapping(mock_clickhouse_conn.conn_id, profile_args={})

    assert profile_mapping.profile == {
        "type": "clickhouse",
        "schema": mock_clickhouse_conn.schema,
        "user": mock_clickhouse_conn.login,
        "password": "{{ env_var('COSMOS_CONN_GENERIC_PASSWORD') }}",
        "driver": "native",
        "port": 9000,
        "host": mock_clickhouse_conn.host,
        "secure": False,
        "clickhouse": "True",
    }


def test_mock_profile() -> None:
    """Tests that the mock_profile values get set correctly."""
    profile_mapping = ClickhouseUserPasswordProfileMapping(
        "conn_id"
    )  # get_automatic_profile_mapping("mock_clickhouse_conn.conn_id", profile_args={})

    assert profile_mapping.mock_profile == {
        "type": "clickhouse",
        "schema": "mock_value",
        "user": "mock_value",
        "driver": "native",
        "port": 9000,
        "host": "mock_value",
        "secure": False,
        "clickhouse": "mock_value",
    }


def test_profile_env_vars(mock_clickhouse_conn: Connection) -> None:
    """Tests that the environment variables get set correctly."""
    profile_mapping = get_automatic_profile_mapping(mock_clickhouse_conn.conn_id, profile_args={})
    assert profile_mapping.env_vars == {"COSMOS_CONN_GENERIC_PASSWORD": mock_clickhouse_conn.password}
