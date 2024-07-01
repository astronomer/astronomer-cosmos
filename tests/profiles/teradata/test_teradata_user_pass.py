"""Tests for the postgres profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.teradata.user_pass import TeradataUserPasswordProfileMapping


@pytest.fixture()
def mock_teradata_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_teradata_connection",
        conn_type="teradata",
        host="my_host",
        login="my_user",
        password="my_password",
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_teradata_conn_custom_tmode():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_teradata_connection",
        conn_type="teradata",
        host="my_host",
        login="my_user",
        password="my_password",
        port=1025,
        schema="my_database",
        extra='{"tmode": "TERA"}',
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the teradata profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == teradata
    # and the following exist:
    # - host
    # - user
    # - password
    potential_values = {
        "conn_type": "teradata",
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

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = TeradataUserPasswordProfileMapping(conn)
            assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TeradataUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_teradata_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_teradata_conn.conn_id,
    )
    print(profile_mapping)
    print(profile_mapping.profile)
    assert isinstance(profile_mapping, TeradataUserPasswordProfileMapping)


def test_profile_mapping_keeps_custom_tmode(mock_teradata_conn_custom_tmode: Connection) -> None:
    profile = TeradataUserPasswordProfileMapping(mock_teradata_conn_custom_tmode.conn_id)
    assert profile.profile["tmode"] == "TERA"


def test_profile_args(
    mock_teradata_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_teradata_conn.conn_id,
        profile_args={"schema": "my_database"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_database",
    }

    assert profile_mapping.profile == {
        "type": mock_teradata_conn.conn_type,
        "host": mock_teradata_conn.host,
        "user": mock_teradata_conn.login,
        "password": "{{ env_var('COSMOS_CONN_TERADATA_PASSWORD') }}",
        "port": mock_teradata_conn.port,
        "schema": "my_database",
    }


def test_profile_args_overrides(
    mock_teradata_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_teradata_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_teradata_conn.conn_type,
        "host": mock_teradata_conn.host,
        "user": mock_teradata_conn.login,
        "password": "{{ env_var('COSMOS_CONN_TERADATA_PASSWORD') }}",
        "port": mock_teradata_conn.port,
        "schema": "my_schema",
    }


def test_profile_env_vars(
    mock_teradata_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_teradata_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_TERADATA_PASSWORD": mock_teradata_conn.password,
    }


def test_mock_profile() -> None:
    """
    Tests that the mock profile port value get set correctly.
    """
    profile = TeradataUserPasswordProfileMapping("mock_conn_id")
    assert profile.mock_profile.get("port") == 1025
