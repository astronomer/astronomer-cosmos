"""Tests for the Snowflake user/non-encrypted private key file profile."""

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.snowflake import (
    SnowflakeEncryptedPrivateKeyFilePemProfileMapping,
    SnowflakePrivateKeyFilePemProfileMapping,
)


@pytest.fixture()
def mock_snowflake_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_snowflake_pk_file_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        extra=json.dumps(
            {
                "account": "my_account",
                "region": "my_region",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_file": "path/to/private_key.p8",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Snowflake profile mapping claims the correct connection type.
    """
    potential_values = {
        "conn_type": "snowflake",
        "login": "my_user",
        "schema": "my_database",
        "extra": json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_file": "path/to/private_key.p8",
            }
        ),
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
            assert not profile_mapping.can_claim_connection()

    # missing account
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = json.dumps(
        {"database": "my_database", "warehouse": "my_warehouse", "private_key_file": "path/to/private_key.p8"}
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # missing database
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = json.dumps(
        {"account": "my_account", "warehouse": "my_warehouse", "private_key_file": "path/to/private_key.p8"}
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # missing warehouse
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = json.dumps(
        {"account": "my_account", "database": "my_database", "private_key_file": "path/to/private_key.p8"}
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # missing private_key_file
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = json.dumps({"account": "my_account", "database": "my_database", "warehouse": "my_warehouse"})
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # all required present, no passphrase, no content -> claims
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert profile_mapping.can_claim_connection()


def test_does_not_claim_when_passphrase_present() -> None:
    """
    When a passphrase (Airflow ``password``) is set, the encrypted-file mapping should own the
    connection, not this one.
    """
    conn = Connection(
        conn_id="my_snowflake_pk_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="my_passphrase",
        extra=json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_file": "path/to/private_key.p8",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected by the auto-discovery.
    """
    profile_mapping = get_automatic_profile_mapping(mock_snowflake_conn.conn_id)
    assert isinstance(profile_mapping, SnowflakePrivateKeyFilePemProfileMapping)


def test_encrypted_mapping_wins_when_passphrase_set() -> None:
    """
    Sanity check the dispatch: when a passphrase is present alongside a key path,
    the encrypted-file mapping is selected, not the new non-encrypted one.
    """
    conn = Connection(
        conn_id="my_snowflake_encrypted_file",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="my_passphrase",
        extra=json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_file": "path/to/private_key.p8",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = get_automatic_profile_mapping(conn.conn_id)
        assert isinstance(profile_mapping, SnowflakeEncryptedPrivateKeyFilePemProfileMapping)


def test_profile_args(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly and no passphrase is emitted.
    """
    profile_mapping = get_automatic_profile_mapping(mock_snowflake_conn.conn_id)

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key_path": mock_snowflake_conn.extra_dejson.get("private_key_file"),
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
        "database": mock_snowflake_conn.extra_dejson.get("database"),
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 4,
    }
    assert "private_key_passphrase" not in profile_mapping.profile


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
    assert profile_mapping.profile_args == {"database": "my_db_override"}

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key_path": mock_snowflake_conn.extra_dejson.get("private_key_file"),
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
        "database": "my_db_override",
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 4,
    }


def test_profile_env_vars_empty(
    mock_snowflake_conn: Connection,
) -> None:
    """
    No secret fields are declared, so no env vars should be exported.
    """
    profile_mapping = get_automatic_profile_mapping(mock_snowflake_conn.conn_id)
    assert profile_mapping.env_vars == {}


def test_query_tag() -> None:
    """
    Tests that query_tag from connection extras is mapped to the dbt profile.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        extra=json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_file": "path/to/private_key.p8",
                "query_tag": "my_query_tag",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakePrivateKeyFilePemProfileMapping(conn)
        assert profile_mapping.profile["query_tag"] == "my_query_tag"


def test_query_tag_absent_when_not_set(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that query_tag is omitted from the profile when not set on the connection.
    """
    profile_mapping = get_automatic_profile_mapping(mock_snowflake_conn.conn_id)
    assert "query_tag" not in profile_mapping.profile
