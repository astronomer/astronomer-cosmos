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

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
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
        schema="my_database",
        extra='{"tmode": "TERA"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
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
    potential_values: dict[str, str] = {
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

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = TeradataUserPasswordProfileMapping(conn)
            assert not profile_mapping.can_claim_connection()

    # Even there is no schema, making user as schema as user itself schema in teradata
    conn = Connection(**{k: v for k, v in potential_values.items() if k != "schema"})
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TeradataUserPasswordProfileMapping(conn, {"schema": None})
        assert profile_mapping.can_claim_connection()
    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
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
    assert isinstance(profile_mapping, TeradataUserPasswordProfileMapping)


def test_profile_mapping_schema_validation(mock_teradata_conn: Connection) -> None:
    # port is not handled in airflow connection so adding it as profile_args
    profile = TeradataUserPasswordProfileMapping(mock_teradata_conn.conn_id)
    assert profile.profile["schema"] == "my_user"


def test_profile_mapping_keeps_port(mock_teradata_conn: Connection) -> None:
    # port is not handled in airflow connection so adding it as profile_args
    profile = TeradataUserPasswordProfileMapping(mock_teradata_conn.conn_id, profile_args={"port": 1025})
    assert profile.profile["port"] == 1025


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
    assert profile.mock_profile.get("host") == "mock_value"
    assert profile.mock_profile.get("user") == "mock_value"
    assert profile.mock_profile.get("password") == "mock_value"
    assert profile.mock_profile.get("schema") == "mock_value"
