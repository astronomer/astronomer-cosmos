"""Tests for the sqlserver profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.sqlserver.standard_sqlserver_auth import (
    StandardSQLServerAuth,
)


@pytest.fixture()
def mock_sqlserver_conn():  # type: ignore
    """Sets the connection as an environment variable."""
    conn = Connection(
        conn_id="sqlserver_connection",
        conn_type="generic",
        host="my_host",
        login="my_user",
        port=1433,
        password="my_password",
        schema="dbo",
        extra='{"database": "my_db", "driver": "ODBC Driver 18 for SQL Server"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the sqlserver profile mapping claims the correct connection type.

    should only claim when:
    - conn_type == generic
    And the following exist:
    - host
    - login
    - password
    - schema
    - extra.database
    - extra.driver
    """
    required_values = {
        "conn_type": "generic",
        "host": "my_host",
        "login": "my_user",
        "schema": "dbo",
        "password": "pass",
        "extra": '{"database": "my_db", "driver": "ODBC Driver 18 for SQL Server"}',
    }

    def can_claim_with_missing_key(missing_key: str) -> bool:
        values = required_values.copy()
        del values[missing_key]
        conn = Connection(**values)  # type: ignore
        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = StandardSQLServerAuth(conn, {})
            return profile_mapping.can_claim_connection()

    # if we're missing any of the required values, it shouldn't claim
    for key in required_values:
        assert not can_claim_with_missing_key(key), f"Failed when missing {key}"

    # if we have all the required values, it should claim
    conn = Connection(**required_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = StandardSQLServerAuth(conn, {})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_sqlserver_conn: Connection,
) -> None:
    """Tests that the correct profile mapping is selected."""
    profile_mapping = get_automatic_profile_mapping(mock_sqlserver_conn.conn_id, {})
    assert isinstance(profile_mapping, StandardSQLServerAuth)


def test_profile_args(mock_sqlserver_conn: Connection) -> None:
    """Tests that the profile values get set correctly."""
    profile_mapping = get_automatic_profile_mapping(mock_sqlserver_conn.conn_id, profile_args={})

    assert profile_mapping.profile == {
        "type": "sqlserver",
        "schema": mock_sqlserver_conn.schema,
        "user": mock_sqlserver_conn.login,
        "password": "{{ env_var('COSMOS_CONN_GENERIC_PASSWORD') }}",
        "driver": mock_sqlserver_conn.extra_dejson["driver"],
        "port": mock_sqlserver_conn.port,
        "server": mock_sqlserver_conn.host,
        "database": mock_sqlserver_conn.extra_dejson["database"],
    }


def test_mock_profile() -> None:
    """Tests that the mock_profile values get set correctly."""
    profile_mapping = StandardSQLServerAuth("conn_id")

    assert profile_mapping.mock_profile == {
        "type": "sqlserver",
        "server": "mock_value",
        "schema": "mock_value",
        "database": "mock_value",
        "user": "mock_value",
        "password": "mock_value",
        "driver": "mock_value",
        "port": 1433,
    }


def test_profile_env_vars(mock_sqlserver_conn: Connection) -> None:
    """Tests that the environment variables get set correctly."""
    profile_mapping = get_automatic_profile_mapping(mock_sqlserver_conn.conn_id, profile_args={})
    assert profile_mapping.env_vars == {"COSMOS_CONN_GENERIC_PASSWORD": mock_sqlserver_conn.password}
