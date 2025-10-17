"""Tests for the Trino profile."""

import json
from unittest.mock import patch

from airflow.models.connection import Connection

from cosmos.profiles.trino import TrinoBaseProfileMapping


def test_profile_args() -> None:
    """
    Tests that the profile values get set correctly.
    """
    conn = Connection(
        conn_id="my_trino_conn",
        conn_type="trino",
        host="my_host",
        port=8080,
        login="my_login",
        extra=json.dumps({"session_properties": {"my_property": "my_value"}}),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TrinoBaseProfileMapping(
            conn,
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
            "database": "my_database",
            "schema": "my_schema",
            "host": "my_host",
            "port": 8080,
            "user": "my_login",
            "session_properties": {"my_property": "my_value"},
        }


def test_profile_args_overrides() -> None:
    """
    Tests that you can override the profile values.
    """
    conn = Connection(
        conn_id="my_trino_conn",
        conn_type="trino",
        host="my_host",
        port=8080,
        login="my_login",
        extra=json.dumps({"session_properties": {"my_property": "my_value"}}),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = TrinoBaseProfileMapping(
            conn,
            profile_args={
                "database": "my_database",
                "schema": "my_schema",
                "host": "my_host_override",
                "session_properties": {"my_property": "my_value_override"},
            },
        )
        assert profile_mapping.profile_args == {
            "database": "my_database",
            "schema": "my_schema",
            "host": "my_host_override",
            "session_properties": {"my_property": "my_value_override"},
        }

        assert profile_mapping.profile == {
            "type": "trino",
            "database": "my_database",
            "schema": "my_schema",
            "host": "my_host_override",
            "port": 8080,
            "user": "my_login",
            "session_properties": {"my_property": "my_value_override"},
        }


def test_mock_profile() -> None:
    """
    Tests that the mock_profile values get set correctly. A non-integer value of port will crash dbt ls.
    """
    profile_mapping = TrinoBaseProfileMapping(conn_id="mock_conn_id")

    assert profile_mapping.mock_profile == {
        "type": "trino",
        "host": "mock_value",
        "database": "mock_value",
        "schema": "mock_value",
        "port": 99999,
        "user": "mock_value",
    }
