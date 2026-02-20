"""Tests for the Snowflake user/password profile."""

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.snowflake import (
    SnowflakeUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_snowflake_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_snowflake_password_connection",
        conn_type="snowflake",
        login="my_user",
        password="my_password",
        schema="my_schema",
        extra='{"account": "my_account", "database": "my_database", "warehouse": "my_warehouse"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Snowflake profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == snowflake
    # and the following exist:
    # - user
    # - password
    # - account
    # - database
    # - warehouse
    # - schema
    potential_values = {
        "conn_type": "snowflake",
        "login": "my_user",
        "password": "my_password",
        "schema": "my_database",
        "extra": '{"account": "my_account", "database": "my_database", "warehouse": "my_warehouse"}',
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = SnowflakeUserPasswordProfileMapping(
                conn,
            )
            assert not profile_mapping.can_claim_connection()

    # test when we're missing the account
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"database": "my_database", "warehouse": "my_warehouse"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the database
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "warehouse": "my_warehouse"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the warehouse
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "database": "my_database"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert isinstance(profile_mapping, SnowflakeUserPasswordProfileMapping)


def test_profile_args(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "password": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PASSWORD') }}",
        "schema": mock_snowflake_conn.schema,
        "account": mock_snowflake_conn.extra_dejson.get("account"),
        "database": mock_snowflake_conn.extra_dejson.get("database"),
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 4,
    }


def test_profile_args_overrides(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
        profile_args={"database": "my_db_override"},
    )
    assert profile_mapping.profile_args == {
        "database": "my_db_override",
    }

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "password": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PASSWORD') }}",
        "schema": mock_snowflake_conn.schema,
        "account": mock_snowflake_conn.extra_dejson.get("account"),
        "database": "my_db_override",
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 4,
    }


def test_profile_args_overrides_threads(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that you can override the default threads value via profile_args.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
        profile_args={"threads": 8},
    )

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "password": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PASSWORD') }}",
        "schema": mock_snowflake_conn.schema,
        "account": mock_snowflake_conn.extra_dejson.get("account"),
        "database": mock_snowflake_conn.extra_dejson.get("database"),
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 8,
    }


def test_profile_env_vars(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_SNOWFLAKE_PASSWORD": mock_snowflake_conn.password,
    }


def test_old_snowflake_format() -> None:
    """
    Tests that the old format still works.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        password="my_password",
        schema="my_schema",
        extra=json.dumps(
            {
                "extra__snowflake__account": "my_account",
                "extra__snowflake__database": "my_database",
                "extra__snowflake__warehouse": "my_warehouse",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert profile_mapping.profile == {
            "type": conn.conn_type,
            "user": conn.login,
            "password": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PASSWORD') }}",
            "schema": conn.schema,
            "account": conn.extra_dejson.get("account"),
            "database": conn.extra_dejson.get("database"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "threads": 4,
        }


def test_appends_region() -> None:
    """
    Tests that region is appended to account if it doesn't already exist.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        password="my_password",
        schema="my_schema",
        extra=json.dumps(
            {
                "account": "my_account",
                "region": "my_region",
                "database": "my_database",
                "warehouse": "my_warehouse",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert profile_mapping.profile == {
            "type": conn.conn_type,
            "user": conn.login,
            "password": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PASSWORD') }}",
            "schema": conn.schema,
            "account": f"{conn.extra_dejson.get('account')}.{conn.extra_dejson.get('region')}",
            "database": conn.extra_dejson.get("database"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "threads": 4,
        }


def test_appends_host_and_port() -> None:
    """
    Tests that host/port extras are appended to the connection settings.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        password="my_password",
        schema="my_schema",
        extra=json.dumps(
            {
                "host": "snowflake.localhost.localstack.cloud",
                "port": 4566,
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeUserPasswordProfileMapping(conn)
        assert profile_mapping.profile["host"] == "snowflake.localhost.localstack.cloud"
        assert profile_mapping.profile["port"] == 4566
