"Tests for the Athena profile."

import json
from collections import namedtuple
import sys
from unittest.mock import MagicMock, patch
import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.athena.access_key import AthenaAccessKeyProfileMapping

Credentials = namedtuple("Credentials", ["access_key", "secret_key", "token"])

mock_assumed_credentials = Credentials(
    secret_key="my_aws_assumed_secret_key",
    access_key="my_aws_assumed_access_key",
    token="my_aws_assumed_token",
)

mock_missing_credentials = Credentials(access_key=None, secret_key=None, token=None)


@pytest.fixture(autouse=True)
def mock_aws_module():
    mock_aws_hook = MagicMock()

    class MockAwsGenericHook:
        def __init__(self, conn_id: str) -> None:
            pass

        def get_credentials(self) -> Credentials:
            return mock_assumed_credentials

    mock_aws_hook.AwsGenericHook = MockAwsGenericHook

    with patch.dict(sys.modules, {"airflow.providers.amazon.aws.hooks.base_aws": mock_aws_hook}):
        yield mock_aws_hook


@pytest.fixture()
def mock_athena_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """

    conn = Connection(
        conn_id="my_athena_connection",
        conn_type="aws",
        login="my_aws_access_key_id",
        password="my_aws_secret_key",
        extra=json.dumps(
            {
                "aws_session_token": "token123",
                "database": "my_database",
                "region_name": "us-east-1",
                "s3_staging_dir": "s3://my_bucket/dbt/",
                "schema": "my_schema",
            }
        ),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_athena_connection_claiming() -> None:
    """
    Tests that the Athena profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == aws
    # and the following exist:
    # - login
    # - password
    # - database
    # - region_name
    # - s3_staging_dir
    # - schema

    potential_values = {
        "conn_type": "aws",
        "login": "my_aws_access_key_id",
        "password": "my_aws_secret_key",
        "extra": json.dumps(
            {
                "database": "my_database",
                "region_name": "us-east-1",
                "s3_staging_dir": "s3://my_bucket/dbt/",
                "schema": "my_schema",
            }
        ),
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        with patch(
            "cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping._get_temporary_credentials",
            return_value=mock_missing_credentials,
        ):
            with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
                # should raise an InvalidMappingException
                profile_mapping = AthenaAccessKeyProfileMapping(conn, {})
                assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = AthenaAccessKeyProfileMapping(conn, {})
        assert profile_mapping.can_claim_connection()


def test_athena_profile_mapping_selected(
    mock_athena_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected for Athena.
    """

    profile_mapping = get_automatic_profile_mapping(
        mock_athena_conn.conn_id,
    )
    assert isinstance(profile_mapping, AthenaAccessKeyProfileMapping)


def test_athena_profile_args(
    mock_athena_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly for Athena.
    """

    profile_mapping = get_automatic_profile_mapping(
        mock_athena_conn.conn_id,
    )

    assert profile_mapping.profile == {
        "type": "athena",
        "aws_access_key_id": mock_assumed_credentials.access_key,
        "aws_secret_access_key": "{{ env_var('COSMOS_CONN_AWS_AWS_SECRET_ACCESS_KEY') }}",
        "aws_session_token": "{{ env_var('COSMOS_CONN_AWS_AWS_SESSION_TOKEN') }}",
        "database": mock_athena_conn.extra_dejson.get("database"),
        "region_name": mock_athena_conn.extra_dejson.get("region_name"),
        "s3_staging_dir": mock_athena_conn.extra_dejson.get("s3_staging_dir"),
        "schema": mock_athena_conn.extra_dejson.get("schema"),
    }


def test_athena_profile_args_overrides(
    mock_athena_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values for Athena.
    """

    profile_mapping = get_automatic_profile_mapping(
        mock_athena_conn.conn_id,
        profile_args={
            "schema": "my_custom_schema",
            "database": "my_custom_db",
            "aws_session_token": "override_token",
        },
    )

    assert profile_mapping.profile_args == {
        "schema": "my_custom_schema",
        "database": "my_custom_db",
        "aws_session_token": "override_token",
    }

    assert profile_mapping.profile == {
        "type": "athena",
        "aws_access_key_id": mock_assumed_credentials.access_key,
        "aws_secret_access_key": "{{ env_var('COSMOS_CONN_AWS_AWS_SECRET_ACCESS_KEY') }}",
        "aws_session_token": "{{ env_var('COSMOS_CONN_AWS_AWS_SESSION_TOKEN') }}",
        "database": "my_custom_db",
        "region_name": mock_athena_conn.extra_dejson.get("region_name"),
        "s3_staging_dir": mock_athena_conn.extra_dejson.get("s3_staging_dir"),
        "schema": "my_custom_schema",
    }


def test_athena_profile_env_vars(
    mock_athena_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly for Athena.
    """

    profile_mapping = get_automatic_profile_mapping(
        mock_athena_conn.conn_id,
    )

    assert profile_mapping.env_vars == {
        "COSMOS_CONN_AWS_AWS_SECRET_ACCESS_KEY": mock_assumed_credentials.secret_key,
        "COSMOS_CONN_AWS_AWS_SESSION_TOKEN": mock_assumed_credentials.token,
    }
