"""Tests for the starrocks profile mapping."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.starrocks.user_pass import StarrocksUserPasswordProfileMapping


@pytest.fixture()
def mock_starrocks_conn():  # type: ignore
    """
    Mocks a StarRocks connection via Airflow MySQL connection (StarRocks speaks MySQL protocol).
    """
    conn = Connection(
        conn_id="my_starrocks_connection",
        conn_type="mysql",
        host="my_host",
        login="my_user",
        password="my_password",
        port=9030,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_starrocks_conn_custom_port():  # type: ignore
    """
    Same as above but with a custom port.
    """
    conn = Connection(
        conn_id="my_starrocks_connection",
        conn_type="mysql",
        host="my_host",
        login="my_user",
        password="my_password",
        port=8040,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the starrocks profile mapping claims the correct connection type and required fields.
    """
    # should only claim when:
    # - conn_type == mysql
    # - host/login/password/port/schema are present
    potential_values = {
        "conn_type": "mysql",
        "host": "my_host",
        "login": "my_user",
        "password": "my_password",
        "port": 9030,
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = StarrocksUserPasswordProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when schema is explicitly None in profile_args
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = StarrocksUserPasswordProfileMapping(conn, {"schema": None})
        assert not profile_mapping.can_claim_connection()

    # if we have them all and provide schema in profile_args, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = StarrocksUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_keeps_custom_port(mock_starrocks_conn_custom_port: Connection) -> None:
    profile_mapping = StarrocksUserPasswordProfileMapping(
        mock_starrocks_conn_custom_port.conn_id,
        {"schema": "my_schema"},
    )
    assert profile_mapping.profile["port"] == 8040


def test_profile_args_and_profile(mock_starrocks_conn: Connection) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = StarrocksUserPasswordProfileMapping(
        mock_starrocks_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )

    assert profile_mapping.profile_args == {"schema": "my_schema"}

    # NOTE: env var name is based on airflow_connection_type (mysql), not dbt_profile_type (starrocks)
    assert profile_mapping.profile == {
        "type": "starrocks",
        "host": mock_starrocks_conn.host,
        "username": mock_starrocks_conn.login,
        "password": "{{ env_var('COSMOS_CONN_MYSQL_PASSWORD') }}",
        "port": mock_starrocks_conn.port,
        "schema": "my_schema",
    }


def test_profile_env_vars(mock_starrocks_conn: Connection) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = StarrocksUserPasswordProfileMapping(
        mock_starrocks_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_MYSQL_PASSWORD": mock_starrocks_conn.password,
    }


def test_mock_profile(mock_starrocks_conn: Connection) -> None:
    """
    Tests that the mock profile is generated correctly.
    """
    profile_mapping = StarrocksUserPasswordProfileMapping(
        mock_starrocks_conn.conn_id,
        {"schema": "my_schema"},
    )
    mock_profile = profile_mapping.mock_profile
    assert mock_profile["port"] == 9030
    assert mock_profile["schema"] == "my_schema"
