"""Tests for the duckdb profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.duckdb.user_pass import (
    DuckDBUserPasswordProfileMapping,
)


@pytest.fixture()
def mock_duckdb_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="duckdb",
        conn_type="duckdb",
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the duckdb profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == duckdb
    # and the following exist
    # - path
    required_values = {
        "conn_type": "duckdb",
    }

    # if we have the conn type of duckdb, it should claim
    conn = Connection(**required_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DuckDBUserPasswordProfileMapping(conn, profile_args={"path": "jaffle_shop.duck_db"})
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_duckdb_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_duckdb_conn.conn_id, profile_args={"path": "jaffle_shop.duck_db"}
    )
    assert isinstance(profile_mapping, DuckDBUserPasswordProfileMapping)


def test_profile_args(
    mock_duckdb_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_duckdb_conn.conn_id, profile_args={"path": "jaffle_shop.duck_db"}
    )
    assert profile_mapping.profile_args == {"path": "jaffle_shop.duck_db"}

    assert profile_mapping.profile == {"type": mock_duckdb_conn.conn_type, "path": "jaffle_shop.duck_db"}


def test_profile_args_overrides(
    mock_duckdb_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_duckdb_conn.conn_id,
        profile_args={"path": "jaffle_shop_override.duck_db"},
    )
    assert profile_mapping.profile_args == {"path": "jaffle_shop_override.duck_db"}

    assert profile_mapping.profile == {"type": mock_duckdb_conn.conn_type, "path": "jaffle_shop_override.duck_db"}
