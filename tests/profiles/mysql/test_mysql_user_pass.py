"""Tests for the mysql profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.mysql.user_pass import (
    MysqlUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_mysql_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_mysql_connection",
        conn_type="mysql",
        host="my_host",
        login="my_user",
        password="my_password",
        port=3306,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_mysql_conn_custom_port():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_mysql_connection",
        conn_type="mysql",
        host="my_host",
        login="my_user",
        password="my_password",
        port=5543,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the mysql profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == mysql
    potential_values = {
        "conn_type": "mysql",
        "host": "my_host",
        "login": "my_user",
        "password": "my_password",
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = MysqlUserPasswordProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**{k: v for k, v in potential_values.items() if k != "schema"})
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = MysqlUserPasswordProfileMapping(conn, {"schema": None})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = MysqlUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_mysql_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_mysql_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, MysqlUserPasswordProfileMapping)


def test_profile_mapping_keeps_custom_port(mock_mysql_conn_custom_port: Connection) -> None:
    profile = MysqlUserPasswordProfileMapping(mock_mysql_conn_custom_port.conn_id, {"schema": "my_schema"})
    assert profile.profile["port"] == 5543


def test_profile_args(
    mock_mysql_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_mysql_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_mysql_conn.conn_type,
        "server": mock_mysql_conn.host,
        "username": mock_mysql_conn.login,
        "password": "{{ env_var('COSMOS_CONN_MYSQL_PASSWORD') }}",
        "port": mock_mysql_conn.port,
        "schema": "my_schema",
    }


def test_profile_args_overrides(
    mock_mysql_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_mysql_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_mysql_conn.conn_type,
        "server": mock_mysql_conn.host,
        "username": mock_mysql_conn.login,
        "password": "{{ env_var('COSMOS_CONN_MYSQL_PASSWORD') }}",
        "port": mock_mysql_conn.port,
        "schema": "my_schema",
    }


def test_profile_env_vars(
    mock_mysql_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_mysql_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_MYSQL_PASSWORD": mock_mysql_conn.password,
    }


def test_mock_profile(
    mock_mysql_conn: Connection,
) -> None:
    """
    Tests that the mock profile is generated correctly.
    """
    profile_mapping = MysqlUserPasswordProfileMapping(mock_mysql_conn.conn_id, {"schema": "my_schema"})
    mock_profile = profile_mapping.mock_profile
    assert mock_profile["port"] == 3306
    assert mock_profile["schema"] == "my_schema"
