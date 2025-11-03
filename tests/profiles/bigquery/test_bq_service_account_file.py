"""Tests for the BigQuery profile."""

import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.bigquery.service_account_file import (
    GoogleCloudServiceAccountFileProfileMapping,
)


@pytest.fixture()
def mock_bigquery_conn():  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {
        "project": "my_project",
        "key_path": "my_key_path.json",
    }
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the BigQuery profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == google-cloud-platform
    # and the following exist:
    # - project
    # - key_path
    # - dataset
    extra = {
        "project": "my_project",
        "key_path": "my_key_path.json",
    }

    # if we're missing any of the values, it shouldn't claim
    for key in extra:
        values = extra.copy()
        del values[key]
        conn = Connection(
            conn_id="my_bigquery_connection",
            conn_type="google_cloud_platform",
            extra=json.dumps(values),
        )

        print("testing with", values)

        with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = GoogleCloudServiceAccountFileProfileMapping(conn, {"dataset": "my_dataset"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = GoogleCloudServiceAccountFileProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have dataset specified in extra, it should claim
    dataset_dict = {"dataset": "my_dataset"}
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps({**extra, **dataset_dict}),
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = GoogleCloudServiceAccountFileProfileMapping(conn, {})
        assert profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )
    with patch("cosmos.profiles.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = GoogleCloudServiceAccountFileProfileMapping(conn, {"dataset": "my_dataset"})
        assert profile_mapping.can_claim_connection()


def test_bigquery_mapping_selected(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        {"dataset": "my_dataset"},
    )
    assert isinstance(profile_mapping, GoogleCloudServiceAccountFileProfileMapping)


def test_profile_args(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
        },
    )
    assert profile_mapping.profile_args == {
        "dataset": "my_dataset",
    }

    assert profile_mapping.profile == {
        "type": "bigquery",
        "method": "service-account",
        "project": mock_bigquery_conn.extra_dejson.get("project"),
        "keyfile": mock_bigquery_conn.extra_dejson.get("key_path"),
        "dataset": "my_dataset",
        "threads": 1,
    }


def test_profile_args_overrides(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
            "threads": 2,
            "project": "my_project_override",
            "keyfile": "my_keyfile_override",
        },
    )
    assert profile_mapping.profile_args == {
        "dataset": "my_dataset",
        "threads": 2,
        "project": "my_project_override",
        "keyfile": "my_keyfile_override",
    }

    assert profile_mapping.profile == {
        "type": "bigquery",
        "method": "service-account",
        "project": "my_project_override",
        "keyfile": "my_keyfile_override",
        "dataset": "my_dataset",
        "threads": 2,
    }


def test_profile_env_vars(
    mock_bigquery_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={"dataset": "my_dataset"},
    )
    assert profile_mapping.env_vars == {}
