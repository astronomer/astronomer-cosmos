"Tests for the Trino profile."

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.trino.certificate import (
    TrinoCertificateProfileMapping,
)


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
                "certs__client_cert_path": "/path/to/client_cert",
                "certs__client_key_path": "/path/to/client_key",
            }
        ),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_profile_args(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = TrinoCertificateProfileMapping(
        mock_trino_conn.conn_id,
        profile_args={
            "database": "my_database",
            "schema": "my_schema",
        },
    )

    assert profile_mapping.profile == {
        "type": "trino",
        "method": "certificate",
        "host": "my_host",
        "port": 8080,
        "database": "my_database",
        "schema": "my_schema",
        "client_certificate": "/path/to/client_cert",
        "client_private_key": "/path/to/client_key",
    }


def test_profile_args_overrides(
    mock_trino_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = TrinoCertificateProfileMapping(
        mock_trino_conn.conn_id,
        profile_args={
            "database": "my_database",
            "schema": "my_schema",
            "host": "my_host_override",
            "client_private_key": "my_client_private_key_override",
        },
    )

    assert profile_mapping.profile == {
        "type": "trino",
        "method": "certificate",
        "host": "my_host_override",
        "port": 8080,
        "database": "my_database",
        "schema": "my_schema",
        "client_certificate": "/path/to/client_cert",
        "client_private_key": "my_client_private_key_override",
    }
