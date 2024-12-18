"""Tests for the Oracle profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.oracle.user_pass import OracleUserPasswordProfileMapping


@pytest.fixture()
def mock_oracle_conn():  # type: ignore
    """
    Sets the Oracle connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_oracle_connection",
        conn_type="oracle",
        host="my_host",
        login="my_user",
        password="my_password",
        port=1521,
        extra='{"service_name": "my_service"}',
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_oracle_conn_custom_port():  # type: ignore
    """
    Sets the Oracle connection with a custom port as an environment variable.
    """
    conn = Connection(
        conn_id="my_oracle_connection",
        conn_type="oracle",
        host="my_host",
        login="my_user",
        password="my_password",
        port=1600,
        extra='{"service_name": "my_service"}',
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Oracle profile mapping claims the correct connection type.
    """
    potential_values = {
        "conn_type": "oracle",
        "login": "my_user",
        "password": "my_password",
    }

    # if we're missing any of the required values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = OracleUserPasswordProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # if we have all the required values, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = OracleUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_oracle_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_oracle_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, OracleUserPasswordProfileMapping)


def test_profile_mapping_keeps_custom_port(mock_oracle_conn_custom_port: Connection) -> None:
    profile = OracleUserPasswordProfileMapping(mock_oracle_conn_custom_port.conn_id, {"schema": "my_schema"})
    assert profile.profile["port"] == 1600


def test_profile_args(
    mock_oracle_conn: Connection,
) -> None:
    """
    Tests that the profile values are set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_oracle_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_oracle_conn.conn_type,
        "host": mock_oracle_conn.host,
        "user": mock_oracle_conn.login,
        "password": "{{ env_var('COSMOS_CONN_ORACLE_PASSWORD') }}",
        "port": mock_oracle_conn.port,
        "database": "my_service",
        "service": "my_service",
        "schema": "my_schema",
        "protocol": "tcp",
    }


def test_profile_args_overrides(
    mock_oracle_conn: Connection,
) -> None:
    """
    Tests that profile values can be overridden.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_oracle_conn.conn_id,
        profile_args={
            "schema": "my_schema_override",
            "database": "my_database_override",
            "service": "my_service_override",
        },
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema_override",
        "database": "my_database_override",
        "service": "my_service_override",
    }

    assert profile_mapping.profile == {
        "type": mock_oracle_conn.conn_type,
        "host": mock_oracle_conn.host,
        "user": mock_oracle_conn.login,
        "password": "{{ env_var('COSMOS_CONN_ORACLE_PASSWORD') }}",
        "port": mock_oracle_conn.port,
        "database": "my_database_override",
        "service": "my_service_override",
        "schema": "my_schema_override",
        "protocol": "tcp",
    }


def test_profile_env_vars(
    mock_oracle_conn: Connection,
) -> None:
    """
    Tests that environment variables are set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_oracle_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_ORACLE_PASSWORD": mock_oracle_conn.password,
    }


def test_env_vars_thick_mode(mock_oracle_conn: Connection) -> None:
    """
    Tests that `env_vars` includes `ORA_PYTHON_DRIVER_TYPE` when `extra.thick_mode` is enabled.
    """
    mock_oracle_conn.extra = '{"service_name": "my_service", "thick_mode": true}'
    profile_mapping = OracleUserPasswordProfileMapping(mock_oracle_conn.conn_id, {"schema": "my_schema"})
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_ORACLE_PASSWORD": mock_oracle_conn.password,
        "ORA_PYTHON_DRIVER_TYPE": "thick",
    }


def test_profile_filter_null(mock_oracle_conn: Connection) -> None:
    """
    Tests that `profile` filters out null values.
    """
    mock_oracle_conn.extra = '{"service_name": "my_service"}'
    profile_mapping = OracleUserPasswordProfileMapping(mock_oracle_conn.conn_id, {"schema": None})
    profile = profile_mapping.profile
    assert "schema" not in profile


def test_mock_profile(mock_oracle_conn: Connection) -> None:
    """
    Tests that `mock_profile` sets default port and schema correctly.
    """
    profile_mapping = OracleUserPasswordProfileMapping(mock_oracle_conn.conn_id, {"schema": "my_schema"})
    mock_profile = profile_mapping.mock_profile
    assert mock_profile["port"] == 1521
    assert mock_profile["schema"] == "my_schema"
    assert mock_profile["protocol"] == "tcp"


def test_invalid_connection_type() -> None:
    """
    Tests that the profile mapping does not claim a non-oracle connection type.
    """
    conn = Connection(conn_id="invalid_conn", conn_type="postgres", login="my_user", password="my_password")
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = OracleUserPasswordProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()


def test_airflow_param_mapping(mock_oracle_conn: Connection) -> None:
    """
    Tests that `airflow_param_mapping` correctly maps Airflow fields to dbt profile fields.
    """
    profile_mapping = OracleUserPasswordProfileMapping(mock_oracle_conn.conn_id, {"schema": "my_schema"})
    mapped_params = profile_mapping.mapped_params

    assert mapped_params["host"] == mock_oracle_conn.host
    assert mapped_params["port"] == mock_oracle_conn.port
    assert mapped_params["service"] == "my_service"
    assert mapped_params["user"] == mock_oracle_conn.login
    assert mapped_params["password"] == mock_oracle_conn.password
