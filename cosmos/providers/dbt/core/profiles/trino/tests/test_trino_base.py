"Tests for the Trino profile."

import json

from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles.trino.base import TrinoBaseProfileMapping


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

    assert profile_mapping.get_profile() == {
        "type": "trino",
        "database": "my_database",
        "schema": "my_schema",
        "host": "my_host",
        "port": 8080,
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

    assert profile_mapping.get_profile() == {
        "type": "trino",
        "database": "my_database",
        "schema": "my_schema",
        "host": "my_host_override",
        "port": 8080,
        "session_properties": {"my_property": "my_value_override"},
    }
