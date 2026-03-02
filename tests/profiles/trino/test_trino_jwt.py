"""Tests for the Trino profile."""

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import TrinoJWTProfileMapping, get_automatic_profile_mapping


@pytest.fixture()
def mock_trino_conn():  # type: ignore
    """
    Mocks and returns an Airflow Trino connection.
    """
    conn = Connection(
        conn_id="my_trino_conn",
        conn_type="trino",
        host="my_host",
        port=8080,
        extra=json.dumps(
            {
                "jwt__token": "my_jwt_token",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Trino JWT profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == trino
    # and the following exist:
    # - host
    # - database
    # - schema
    # - port
    # - jwt
    potential_values = {
        "conn_type": "trino",
        "host": "my_host",
        "port": 8080,
        "extra": json.dumps(
            {
                "jwt__token": "my_jwt_token",
            }
        ),
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = TrinoJWTProfileMapping(conn, {"database": "my_database", "schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TrinoJWTProfileMapping(
            conn,
            {
                "database": "my_database",
            },
        )
        assert not profile_mapping.can_claim_connection()

    # also test when there's no database
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TrinoJWTProfileMapping(
            conn,
            {
                "schema": "my_schema",
            },
        )
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TrinoJWTProfileMapping(
            conn,
            {
                "database": "my_database",
                "schema": "my_schema",
            },
        )
        assert profile_mapping.can_claim_connection()


def test_trino_mapping_selected(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_trino_conn.conn_id,
        {
            "database": "my_database",
            "schema": "my_schema",
        },
    )
    assert isinstance(profile_mapping, TrinoJWTProfileMapping)


def test_profile_args(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_trino_conn.conn_id,
        profile_args={
            "database": "my_database",
            "schema": "my_schema",
        },
    )
    assert profile_mapping.profile_args == {
        "database": "my_database",
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": "trino",
        "method": "jwt",
        "host": "my_host",
        "port": 8080,
        "database": "my_database",
        "schema": "my_schema",
        "jwt_token": "{{ env_var('COSMOS_CONN_TRINO_JWT_TOKEN') }}",
    }


def test_profile_args_overrides(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_trino_conn.conn_id,
        profile_args={
            "database": "my_database",
            "schema": "my_schema",
            "host": "my_host_override",
            "jwt_token": "my_jwt_token_override",
        },
    )
    assert profile_mapping.profile_args == {
        "database": "my_database",
        "schema": "my_schema",
        "host": "my_host_override",
        "jwt_token": "my_jwt_token_override",
    }

    assert profile_mapping.profile == {
        "type": "trino",
        "method": "jwt",
        "host": "my_host_override",
        "port": 8080,
        "database": "my_database",
        "schema": "my_schema",
        "jwt_token": "{{ env_var('COSMOS_CONN_TRINO_JWT_TOKEN') }}",
    }


def test_profile_env_vars(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_trino_conn.conn_id,
        profile_args={
            "database": "my_database",
            "schema": "my_schema",
        },
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_TRINO_JWT_TOKEN": "my_jwt_token",
    }
