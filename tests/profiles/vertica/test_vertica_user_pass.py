"""Tests for the vertica profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.vertica.user_pass import (
    VerticaUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_vertica_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_vertica_connection",
        conn_type="vertica",
        host="my_host",
        login="my_user",
        password="my_password",
        port=5433,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_vertica_conn_custom_port():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_vertica_connection",
        conn_type="vertica",
        host="my_host",
        login="my_user",
        password="my_password",
        port=7472,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the vertica profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == vertica
    # and the following exist:
    # - host
    # - username
    # - password
    # - port
    # - database or database
    # - schema
    potential_values = {
        "conn_type": "vertica",
        "host": "my_host",
        "login": "my_user",
        "password": "my_password",
        "schema": "my_database",
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = VerticaUserPasswordProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = ""
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = VerticaUserPasswordProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = VerticaUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_vertica_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_vertica_conn.conn_id,
        {"schema": "my_database"},
    )
    assert isinstance(profile_mapping, VerticaUserPasswordProfileMapping)


def test_mock_profile() -> None:
    """
    Tests that the mock profile port value get set correctly.
    """
    profile = VerticaUserPasswordProfileMapping("mock_conn_id")
    assert profile.mock_profile.get("port") == 5433


def test_profile_mapping_keeps_custom_port(mock_vertica_conn_custom_port: Connection) -> None:
    profile = VerticaUserPasswordProfileMapping(mock_vertica_conn_custom_port.conn_id, {"schema": "my_schema"})
    assert profile.profile["port"] == 7472


def test_profile_args(
    mock_vertica_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_vertica_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_vertica_conn.conn_type,
        "host": mock_vertica_conn.host,
        "username": mock_vertica_conn.login,
        "password": "{{ env_var('COSMOS_CONN_VERTICA_PASSWORD') }}",
        "port": mock_vertica_conn.port,
        "database": mock_vertica_conn.schema,
        "schema": "my_schema",
    }


def test_profile_args_overrides(
    mock_vertica_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_vertica_conn.conn_id,
        profile_args={"schema": "my_schema", "database": "my_db_override"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "database": "my_db_override",
    }

    assert profile_mapping.profile == {
        "type": mock_vertica_conn.conn_type,
        "host": mock_vertica_conn.host,
        "username": mock_vertica_conn.login,
        "password": "{{ env_var('COSMOS_CONN_VERTICA_PASSWORD') }}",
        "port": mock_vertica_conn.port,
        "database": "my_db_override",
        "schema": "my_schema",
    }


def test_profile_env_vars(
    mock_vertica_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_vertica_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_VERTICA_PASSWORD": mock_vertica_conn.password,
    }
