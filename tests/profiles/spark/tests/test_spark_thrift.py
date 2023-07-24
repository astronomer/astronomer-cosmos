"Tests for the Spark profile."

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles.spark import SparkThriftProfileMapping


@pytest.fixture()
def mock_spark_conn():  # type: ignore
    """
    Mocks and returns an Airflow Spark connection.
    """
    conn = Connection(conn_id="my_spark_conn", conn_type="spark", host="my_host")

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_profile_args(
    mock_spark_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = SparkThriftProfileMapping(
        mock_spark_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )

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
    profile_mapping = SparkThriftProfileMapping(
        mock_spark_conn.conn_id,
        profile_args={
            "schema": "my_schema",
            "host": "my_host_override",
            "user": "my_user",
        },
    )

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
    profile_mapping = SparkThriftProfileMapping(
        mock_spark_conn.conn_id,
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.env_vars == {}
