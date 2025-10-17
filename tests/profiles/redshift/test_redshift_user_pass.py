"""Tests for the Redshift profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.redshift.user_pass import (
    RedshiftUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_redshift_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_redshift_connection",
        conn_type="redshift",
        host="my_host",
        login="my_user",
        password="my_password",
        port=5439,
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Redshift profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == redshift
    # and the following exist:
    # - host
    # - user
    # - password
    # - dbname or database
    # - schema
    potential_values = {
        "conn_type": "redshift",
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
            # should raise an InvalidMappingException
            profile_mapping = RedshiftUserPasswordProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = RedshiftUserPasswordProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = RedshiftUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_redshift_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_redshift_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, RedshiftUserPasswordProfileMapping)


def test_profile_args(
    mock_redshift_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_redshift_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": mock_redshift_conn.conn_type,
        "host": mock_redshift_conn.host,
        "user": mock_redshift_conn.login,
        "password": "{{ env_var('COSMOS_CONN_REDSHIFT_PASSWORD') }}",
        "port": mock_redshift_conn.port,
        "dbname": mock_redshift_conn.schema,
        "schema": "my_schema",
    }


def test_profile_args_overrides(
    mock_redshift_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_redshift_conn.conn_id,
        profile_args={"schema": "my_schema", "dbname": "my_db_override"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "dbname": "my_db_override",
    }

    assert profile_mapping.profile == {
        "type": mock_redshift_conn.conn_type,
        "host": mock_redshift_conn.host,
        "user": mock_redshift_conn.login,
        "password": "{{ env_var('COSMOS_CONN_REDSHIFT_PASSWORD') }}",
        "port": mock_redshift_conn.port,
        "dbname": "my_db_override",
        "schema": "my_schema",
    }


def test_profile_env_vars(
    mock_redshift_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_redshift_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_REDSHIFT_PASSWORD": mock_redshift_conn.password,
    }
