"Tests for the Redshift profile."

from unittest.mock import patch
import pytest

from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles import get_profile_mapping
from cosmos.providers.dbt.core.profiles.base import InvalidMappingException
from cosmos.providers.dbt.core.profiles.redshift import RedshiftPasswordProfileMapping


@pytest.fixture()
def _mock_redshift_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_redshift_connection",
        conn_type="redshift",
        host="my_host",
        login="my_user",
        password="my_password",
        port=5439,
        schema="my_database",
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield


def test_connection_claiming() -> None:
    """
    Tests that the Redshift profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == redshift
    # and the following exist:
    # - host
    # - user
    # - password
    # - port
    # - dbname or database
    # - schema
    potential_values = {
        "conn_type": "redshift",
        "host": "my_host",
        "login": "my_user",
        "password": "my_password",
        "port": 5439,
        "schema": "my_database",
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print('testing with', values)

        # should raise an InvalidMappingException
        with pytest.raises(InvalidMappingException):
            profile_mapping = RedshiftPasswordProfileMapping(
                conn, {"schema": "my_schema"}
            )

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with pytest.raises(InvalidMappingException):
        profile_mapping = RedshiftPasswordProfileMapping(conn, {})

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    profile_mapping = RedshiftPasswordProfileMapping(
        conn, {"schema": "my_schema"})
    assert profile_mapping.validate_connection()


def test_profile_mapping_selected(
    _mock_redshift_conn: None,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_profile_mapping(
        "my_redshift_connection",
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, RedshiftPasswordProfileMapping)


def test_profile_args(
    _mock_redshift_conn: None,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_profile_mapping(
        "my_redshift_connection",
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
    }

    assert profile_mapping.get_profile() == {
        "type": "redshift",
        "host": "my_host",
        "user": "my_user",
        "password": "{{ env_var('COSMOS_CONN_REDSHIFT_PASSWORD') }}",
        "port": 5439,
        "dbname": "my_database",
        "schema": "my_schema",
    }


def test_profile_args_overrides(
    _mock_redshift_conn: None,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_profile_mapping(
        "my_redshift_connection",
        profile_args={"schema": "my_schema", "dbname": "my_db_override"},
    )
    assert profile_mapping.profile_args == {
        "schema": "my_schema",
        "dbname": "my_db_override",
    }

    assert profile_mapping.get_profile() == {
        "type": "redshift",
        "host": "my_host",
        "user": "my_user",
        "password": "{{ env_var('COSMOS_CONN_REDSHIFT_PASSWORD') }}",
        "port": 5439,
        "dbname": "my_db_override",
        "schema": "my_schema",
    }


def test_profile_env_vars(
    _mock_redshift_conn: None,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_profile_mapping(
        "my_redshift_connection",
        profile_args={"schema": "my_schema"},
    )
    assert profile_mapping.get_env_vars() == {
        "COSMOS_CONN_REDSHIFT_PASSWORD": "my_password",
    }
