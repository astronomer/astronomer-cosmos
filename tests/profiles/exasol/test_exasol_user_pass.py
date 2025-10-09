"""Tests for the Exasol profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.exasol.user_pass import (
    ExasolUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_exasol_connection():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_exasol_connection",
        conn_type="exasol",
        host="my_host",
        login="my_user",
        password="my_password",
        port=8563,
        schema="my_database",
        extra='{"protocol_version": "1"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Exasol profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == exasol
    # and the following exist:
    # - threads
    # - dsn
    # - user
    # - password
    # - dbname
    # - schema
    potential_values = {
        "conn_type": "exasol",
        "host": "my_host:8563",
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
            profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema", "threads": 1})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"threads": 1})
        assert not profile_mapping.can_claim_connection()

    # also test when there's no threads
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema"})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema", "threads": 1})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_exasol_connection: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_exasol_connection.conn_id, {"schema": "my_schema", "threads": 1}
    )
    assert isinstance(profile_mapping, ExasolUserPasswordProfileMapping)


def test_profile_args(
    mock_exasol_connection: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_exasol_connection.conn_id,
        profile_args={"schema": "my_schema", "threads": 1},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "threads": 1,
    }

    assert profile_mapping.profile == {
        "type": mock_exasol_connection.conn_type,
        "dsn": f"{mock_exasol_connection.host}:{mock_exasol_connection.port}",
        "user": mock_exasol_connection.login,
        "password": "{{ env_var('COSMOS_CONN_EXASOL_PASSWORD') }}",
        "dbname": mock_exasol_connection.schema,
        "schema": "my_schema",
        "threads": 1,
        "protocol_version": "1",
    }


def test_profile_args_overrides(
    mock_exasol_connection: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_exasol_connection.conn_id,
        profile_args={
            "dsn": "my_dsn_override",
            "user": "my_user_override",
            "password": "my_password_override",
            "schema": "my_schema",
            "dbname": "my_db_override",
            "threads": 1,
            "protocol_version": "2",
        },
    )
    assert profile_mapping.profile_args == {
        "dsn": "my_dsn_override",
        "user": "my_user_override",
        "password": "my_password_override",
        "schema": "my_schema",
        "dbname": "my_db_override",
        "threads": 1,
        "protocol_version": "2",
    }

    assert profile_mapping.profile == {
        "type": mock_exasol_connection.conn_type,
        "dsn": "my_dsn_override",
        "user": "my_user_override",
        "password": "{{ env_var('COSMOS_CONN_EXASOL_PASSWORD') }}",
        "dbname": "my_db_override",
        "schema": "my_schema",
        "threads": 1,
        "protocol_version": "2",
    }


def test_profile_env_vars(
    mock_exasol_connection: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_exasol_connection.conn_id,
        profile_args={"schema": "my_schema", "threads": 1},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_EXASOL_PASSWORD": mock_exasol_connection.password,
    }


def test_dsn_formatting() -> None:
    """
    Tests that the DSN gets set correctly when a user specifies a port.
    """
    # first, test with a host that includes a port
    conn = Connection(
        conn_id="my_exasol_connection",
        conn_type="exasol",
        host="my_host:1000",
        login="my_user",
        password="my_password",
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema", "threads": 1})
        assert profile_mapping.get_dbt_value("dsn") == "my_host:1000"

    # next, test with a host that doesn't include a port
    conn = Connection(
        conn_id="my_exasol_connection",
        conn_type="exasol",
        host="my_host",
        login="my_user",
        password="my_password",
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema", "threads": 1})
        assert profile_mapping.get_dbt_value("dsn") == "my_host:8563"  # should default to 8563

    # lastly, test with a port override
    conn = Connection(
        conn_id="my_exasol_connection",
        conn_type="exasol",
        host="my_host",
        login="my_user",
        password="my_password",
        port=1000,  # different than the default
        schema="my_database",
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = ExasolUserPasswordProfileMapping(conn, {"schema": "my_schema", "threads": 1})
        assert profile_mapping.get_dbt_value("dsn") == "my_host:1000"
