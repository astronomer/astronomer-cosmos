"""Tests for the postgres profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.databricks import (
    DatabricksTokenProfileMapping,
)


@pytest.fixture()
def mock_databricks_conn():  # type: ignore
    """
    Mocks and returns an Airflow Databricks connection.
    """
    conn = Connection(
        conn_id="my_databricks_connection",
        conn_type="databricks",
        host="https://my_host",
        password="my_token",
        extra='{"http_path": "my_http_path"}',
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Databricks profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == databricks
    # and the following exist:
    # - schema
    # - host
    # - http_path
    # - token
    potential_values = {
        "conn_type": "databricks",
        "host": "my_host",
        "password": "my_token",
        "extra": '{"http_path": "my_http_path"}',
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = DatabricksTokenProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksTokenProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksTokenProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_databricks_mapping_selected(
    mock_databricks_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_databricks_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, DatabricksTokenProfileMapping)


def test_profile_args(
    mock_databricks_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_databricks_conn.conn_id,
        profile_args={
            "schema": "my_schema",
            "catalog": "my_catalog",
            "session_properties": {"legacy_time_parser_policy": "corrected"},
            "threads": 4,
        },
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "catalog": "my_catalog",
        "session_properties": {"legacy_time_parser_policy": "corrected"},
        "threads": 4,
    }

    assert profile_mapping.profile == {
        "type": mock_databricks_conn.conn_type,
        "host": mock_databricks_conn.host.replace("https://", ""),
        "token": "{{ env_var('COSMOS_CONN_DATABRICKS_TOKEN') }}",
        "http_path": mock_databricks_conn.extra_dejson.get("http_path"),
        "schema": "my_schema",
        "catalog": "my_catalog",
        "threads": 4,
        "session_properties": {"legacy_time_parser_policy": "corrected"},
    }
    expected_profile_yml = """example:
    outputs:
        cosmos_target:
            catalog: my_catalog
            host: my_host
            http_path: my_http_path
            schema: my_schema
            session_properties:
                legacy_time_parser_policy: corrected
            threads: 4
            token: '{{ env_var(''COSMOS_CONN_DATABRICKS_TOKEN'') }}'
            type: databricks
    target: cosmos_target\n"""
    assert profile_mapping.get_profile_file_contents("example") == expected_profile_yml


def test_profile_args_overrides(
    mock_databricks_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_databricks_conn.conn_id,
        profile_args={
            "schema": "my_schema",
            "http_path": "http_path_override",
            "host": "my_host_override",
        },
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "http_path": "http_path_override",
        "host": "my_host_override",
    }

    assert profile_mapping.profile == {
        "type": mock_databricks_conn.conn_type,
        "host": "my_host_override",
        "token": "{{ env_var('COSMOS_CONN_DATABRICKS_TOKEN') }}",
        "http_path": "http_path_override",
        "schema": "my_schema",
    }


def test_profile_env_vars(
    mock_databricks_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_databricks_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_DATABRICKS_TOKEN": mock_databricks_conn.password,
    }
