"Tests for the clickhouse profile."

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_profile_mapping
from cosmos.profiles.clickhouse.user_pass import (
    ClickhouseUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_clickhouse_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="clickhouse_connection",
        conn_type="generic",
        host="my_host",
        login="my_user",
        password="my_password",
        schema="my_database",
        extra='{"clickhouse": "True"}',
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the clickhose profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == generic
    # and the following exist:
    # - host
    # - login
    # - password
    # - schema
    # - extra.clickhouse
    potential_values = {
        "conn_type": "generic",
        "host": "my_host",
        "login": "my_user",
        "schema": "my_database",
        "extra": '{"clickhouse": "True"}',
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        profile_mapping = ClickhouseUserPasswordProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    profile_mapping = ClickhouseUserPasswordProfileMapping(conn, {})
    assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_clickhouse_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_profile_mapping(
        mock_clickhouse_conn.conn_id,
        {},
    )
    assert isinstance(profile_mapping, ClickhouseUserPasswordProfileMapping)


def test_profile_args(
    mock_clickhouse_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_profile_mapping(
        mock_clickhouse_conn.conn_id,
        profile_args={},
    )

    assert profile_mapping.profile == {
        "type": "clickhouse",
        "schema": mock_clickhouse_conn.schema,
        "login": mock_clickhouse_conn.login,
        "password": "{{ env_var('COSMOS_CONN_GENERIC_PASSWORD') }}",
        "driver": "native",
        "port": 9000,
        "host": mock_clickhouse_conn.host,
        "secure": False,
        "clickhouse": "True",
    }


def test_profile_env_vars(
    mock_clickhouse_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_profile_mapping(
        mock_clickhouse_conn.conn_id,
        profile_args={},
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_GENERIC_PASSWORD": mock_clickhouse_conn.password,
    }
