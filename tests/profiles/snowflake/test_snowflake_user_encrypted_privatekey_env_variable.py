"""Tests for the Snowflake user/private key environmentvariable profile."""

import base64
import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.snowflake import (
    SnowflakeEncryptedPrivateKeyFilePemProfileMapping,
    SnowflakeEncryptedPrivateKeyPemProfileMapping,
    SnowflakePrivateKeyFilePemProfileMapping,
    SnowflakePrivateKeyPemProfileMapping,
    SnowflakeUserPasswordProfileMapping,
)

PLAIN_TEXT_KEY = "mocked-private-key-content"
BASE64_ENCODED_KEY = base64.b64encode(PLAIN_TEXT_KEY.encode("utf-8")).decode("utf-8")


@pytest.fixture()
def mock_snowflake_conn_base64():  # type: ignore
    """
    Sets a connection with a base64 encoded private key as an environment variable.
    """
    conn = Connection(
        conn_id="my_snowflake_pk_connection_base64",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        extra=json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": BASE64_ENCODED_KEY,
                "private_key_passphrase": "my_private_key_passphrase",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_snowflake_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_snowflake_pk_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="secret",
        extra=json.dumps(
            {
                "account": "my_account",
                "region": "my_region",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": "my_private_key",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.mark.parametrize(
    "input_key, expected_key",
    [
        (BASE64_ENCODED_KEY, PLAIN_TEXT_KEY),
        (PLAIN_TEXT_KEY, PLAIN_TEXT_KEY),
    ],
    ids=["base64_encoded", "plain_text"],
)
def test_decode_private_key_content(input_key: str, expected_key: str, mock_snowflake_conn_base64: Connection) -> None:
    """
    Tests that the private key content is decoded correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn_base64.conn_id,
    )
    assert profile_mapping._decode_private_key_content(input_key) == expected_key


def test_connection_claiming() -> None:
    """
    Tests that the Snowflake profile mapping claims the correct connection type.
    """
    potential_values = {
        "conn_type": "snowflake",
        "login": "my_user",
        "schema": "my_database",
        "password": "secret",
        "extra": json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": "my_private_key",
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
            profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(
                conn,
            )
            assert not profile_mapping.can_claim_connection()

    # test when we're missing the account
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"database": "my_database", "warehouse": "my_warehouse", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the database
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "warehouse": "my_warehouse", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the warehouse
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "database": "my_database", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
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
    assert isinstance(profile_mapping, SnowflakeEncryptedPrivateKeyPemProfileMapping)


def test_profile_args(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
        "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
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

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
        "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
        "database": "my_db_override",
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
        "threads": 4,
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
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY": mock_snowflake_conn.extra_dejson.get("private_key_content"),
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": mock_snowflake_conn.password,
    }


def test_profile_env_vars_with_base64(
    mock_snowflake_conn_base64: Connection,
) -> None:
    """
    Tests that the environment variable get set correctly for a base64-encoded key.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn_base64.conn_id,
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY": PLAIN_TEXT_KEY,
    }


def test_query_tag() -> None:
    """
    Tests that query_tag from connection extras is mapped to the dbt profile.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="secret",
        extra=json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": "my_private_key",
                "query_tag": "my_query_tag",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
        assert profile_mapping.profile["query_tag"] == "my_query_tag"


def test_query_tag_absent_when_not_set(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that query_tag is omitted from the profile when not set on the connection.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert "query_tag" not in profile_mapping.profile


def test_old_snowflake_format() -> None:
    """
    Tests that the old format still works.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="secret",
        extra=json.dumps(
            {
                "extra__snowflake__account": "my_account",
                "extra__snowflake__database": "my_database",
                "extra__snowflake__warehouse": "my_warehouse",
                "extra__snowflake__private_key_content": "my_private_key",
            }
        ),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyPemProfileMapping(conn)
        assert profile_mapping.profile == {
            "type": conn.conn_type,
            "user": conn.login,
            "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
            "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
            "schema": conn.schema,
            "account": conn.extra_dejson.get("account"),
            "database": conn.extra_dejson.get("database"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "threads": 4,
        }


# ---------------------------------------------------------------------------
# Cross-mapping parametrized tests
#
# Each entry pairs a profile mapping class with the connection password and
# extra fields required to satisfy that mapping's ``required_fields``.
# Running the same assertions across all five mappings guards against a
# regression slipping into any single class undetected.
# ---------------------------------------------------------------------------


SNOWFLAKE_PROFILE_MAPPINGS = [
    pytest.param(SnowflakeUserPasswordProfileMapping, "my_password", {}, id="user_password"),
    pytest.param(
        SnowflakePrivateKeyPemProfileMapping,
        None,
        {"private_key_content": "my_private_key"},
        id="private_key_pem",
    ),
    pytest.param(
        SnowflakePrivateKeyFilePemProfileMapping,
        None,
        {"private_key_file": "/path/to/key.p8"},
        id="private_key_file",
    ),
    pytest.param(
        SnowflakeEncryptedPrivateKeyFilePemProfileMapping,
        "my_passphrase",
        {"private_key_file": "/path/to/key.p8"},
        id="encrypted_private_key_file",
    ),
    pytest.param(
        SnowflakeEncryptedPrivateKeyPemProfileMapping,
        "my_passphrase",
        {"private_key_content": "my_private_key"},
        id="encrypted_private_key_pem",
    ),
]


def _build_snowflake_conn(password: str | None, extra_fields: dict, **kwargs) -> Connection:
    """Build a Snowflake connection valid for any of the profile mappings under test."""
    extra = {
        "account": "my_account",
        "database": "my_database",
        "warehouse": "my_warehouse",
        **extra_fields,
        **kwargs,
    }
    return Connection(
        conn_id="my_snowflake_conn",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password=password,
        extra=json.dumps(extra),
    )


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_insecure_mode_from_connection_extra(mapping_class, password, extra_fields) -> None:
    """
    Tests that `insecure_mode` set on the Airflow connection's Extra is forwarded to the profile.

    `insecure_mode` is a first-class field of the Airflow Snowflake connection form. It must reach
    the rendered dbt profile via `airflow_param_mapping` (not only via `profile_args`), otherwise
    users behind PrivateLink who rely on it to bypass OCSP checks silently lose the setting.
    """
    conn = _build_snowflake_conn(password, extra_fields, insecure_mode=True)

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert profile_mapping.profile["insecure_mode"] is True


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_insecure_mode_absent_when_not_set(mapping_class, password, extra_fields) -> None:
    """Tests that `insecure_mode` is omitted from the profile when it is not set on the connection."""
    conn = _build_snowflake_conn(password, extra_fields)

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert "insecure_mode" not in profile_mapping.profile


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_authenticator_from_connection_extra(mapping_class, password, extra_fields) -> None:
    """authenticator set on the Airflow connection Extra is forwarded to every profile mapping."""
    conn = _build_snowflake_conn(password, extra_fields, authenticator="externalbrowser")
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert profile_mapping.profile["authenticator"] == "externalbrowser"


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_authenticator_absent_when_not_set(mapping_class, password, extra_fields) -> None:
    """authenticator is absent from the profile when not set on the connection."""
    conn = _build_snowflake_conn(password, extra_fields)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert "authenticator" not in profile_mapping.profile


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_client_session_keep_alive_from_connection_extra(mapping_class, password, extra_fields) -> None:
    """client_session_keep_alive set on the Airflow connection Extra is forwarded to every profile mapping."""
    conn = _build_snowflake_conn(password, extra_fields, client_session_keep_alive=True)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert profile_mapping.profile["client_session_keep_alive"] is True


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_client_session_keep_alive_absent_when_not_set(mapping_class, password, extra_fields) -> None:
    """client_session_keep_alive is absent from the profile when not set on the connection."""
    conn = _build_snowflake_conn(password, extra_fields)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert "client_session_keep_alive" not in profile_mapping.profile


@pytest.mark.parametrize("mapping_class, password, extra_fields", SNOWFLAKE_PROFILE_MAPPINGS)
def test_host_and_port_from_connection_extra(mapping_class, password, extra_fields) -> None:
    """host and port set on the Airflow connection Extra are forwarded to every profile mapping."""
    conn = _build_snowflake_conn(password, extra_fields, host="my_host.snowflakecomputing.com", port=443)
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = mapping_class(conn)
        assert profile_mapping.profile["host"] == "my_host.snowflakecomputing.com"
        assert profile_mapping.profile["port"] == 443
