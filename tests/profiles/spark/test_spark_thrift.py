"""Tests for the Spark profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.spark import SparkThriftProfileMapping


@pytest.fixture()
def mock_spark_conn():  # type: ignore
    """
    Mocks and returns an Airflow Spark connection.
    """
    conn = Connection(conn_id="my_spark_conn", conn_type="spark", host="my_host")

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Spark profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == spark
    # and the following exist:
    # - host
    # - schema
    potential_values = {
        "conn_type": "spark",
        "host": "my_host",
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = SparkThriftProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SparkThriftProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SparkThriftProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_spark_mapping_selected(
    mock_spark_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_spark_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, SparkThriftProfileMapping)


def test_profile_args(
    mock_spark_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_spark_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.profile == {
        "type": "spark",
        "method": "thrift",
        "schema": "my_schema",
        "host": "my_host",
    }


def test_profile_args_overrides(
    mock_spark_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_spark_conn.conn_id,
        profile_args={
            "schema": "my_schema",
            "host": "my_host_override",
            "user": "my_user",
        },
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "host": "my_host_override",
        "user": "my_user",
    }

    assert profile_mapping.profile == {
        "type": "spark",
        "method": "thrift",
        "schema": "my_schema",
        "host": "my_host_override",
        "user": "my_user",
    }


def test_profile_env_vars(
    mock_spark_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_spark_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {}
