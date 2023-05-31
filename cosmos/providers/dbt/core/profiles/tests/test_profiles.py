import inspect
import json
import os
from pathlib import Path
from typing import Generator, Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml
from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles import (
    create_profile_vars_databricks,
    create_profile_vars_exasol,
    create_profile_vars_google_cloud_platform,
    create_profile_vars_postgres,
    create_profile_vars_redshift,
    create_profile_vars_snowflake,
    create_profile_vars_trino,
    get_available_adapters,
)
from cosmos.providers.dbt.core.profiles.snowflake import get_snowflake_account
from cosmos.providers.dbt.core.utils.profiles_generator import (
    create_default_profiles,
    map_profile,
)


def test_create_default_profiles(tmp_path: Path) -> None:
    """
    Ensure we create the file and that we can return it load it back afterwards.
    """
    profile_file = tmp_path.joinpath("profiles.yml")
    create_default_profiles(profile_file)
    with open(profile_file) as f:
        package_line = next(f)
        assert "astronomer-cosmos" in package_line
    with open(profile_file) as f:
        assert yaml.full_load(f) is not None


def test_create_default_profiles_exist(tmp_path: Path) -> None:
    """
    If the file exists and the version of astronomer-cosmos is the same then do nothing.
    """
    profile_file = tmp_path.joinpath("profiles.yml")
    create_default_profiles(profile_file)
    created_time = os.path.getctime(profile_file)
    create_default_profiles(profile_file)
    modified_time = os.path.getmtime(profile_file)
    assert created_time == modified_time


@patch("cosmos.providers.dbt.core.utils.profiles_generator.cosmos_version")
def test_create_default_profiles_exist_library_update(cosmos_version: MagicMock, tmp_path: Path) -> None:
    """
    If the version of astronomer-cosmos has been updated then we ensure that the profiles are re-written.
    """
    cosmos_version.side_effect = [
        "0.0.1",
        "0.0.2",
    ]
    profile_file = tmp_path.joinpath("profiles.yml")
    create_default_profiles(profile_file)
    created_time = os.path.getctime(profile_file)
    create_default_profiles(profile_file)
    modified_time = os.path.getmtime(profile_file)
    assert created_time != modified_time


@pytest.fixture()
def airflow_connection() -> Generator[Connection, None, None]:
    yield MagicMock(spec=Connection)


@pytest.fixture()
def airflow_schemaless_connection() -> Generator[Connection, None, None]:
    airflow_connection = MagicMock(spec=Connection)
    airflow_connection.schema = None
    yield airflow_connection


@pytest.fixture()
def snowflake_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "snowflake"
    airflow_connection.extra_dejson = {"database": "test_database"}
    yield airflow_connection


@pytest.fixture()
def snowflake_extra_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "snowflake"
    airflow_connection.extra_dejson = {"extra__snowflake__database": "test_database"}
    yield airflow_connection


@pytest.fixture()
def google_cloud_platform_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "google_cloud_platform"
    airflow_connection.extra_dejson = {
        "keyfile_dict": json.dumps(
            {
                "type": "",
                "project_id": "test_database",
                "private_key_id": "",
                "private_key": "",
                "client_email": "",
                "client_id": "",
                "auth_uri": "",
                "token_uri": "",
                "auth_provider_x509_cert_url": "",
                "client_x509_cert_url": "",
            }
        )
    }
    yield airflow_connection


@pytest.fixture()
def redshift_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "redshift"
    airflow_connection.extra_dejson = {"database": "test_database"}
    airflow_connection.schema = "test_schema"
    yield airflow_connection


@pytest.fixture()
def postgres_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "postgres"
    airflow_connection.schema = "test_database"
    yield airflow_connection


@pytest.fixture()
def databricks_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "databricks"
    airflow_connection.schema = "test_database"
    yield airflow_connection


@pytest.fixture()
def random_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "random"
    airflow_connection.schema = "test_database"
    yield airflow_connection


def test_create_profile_vars_databricks(airflow_connection: Connection) -> None:
    catalog = "my-catalog"
    host = "dbc-abcd123-1234.cloud.databricks.com"
    schema = "jaffle_shop"
    http_path = "sql/protocolv1/o/1234567891234567/1234-123456-a1bc2d3e"
    token = "dapiab123456cdefgh78910"
    airflow_connection.host = host
    airflow_connection.schema = schema
    airflow_connection.extra_dejson = {"http_path": http_path, "token": token}
    airflow_connection.password = None

    expected_profile_vars = {
        "DATABRICKS_CATALOG": catalog,
        "DATABRICKS_HOST": host,
        "DATABRICKS_SCHEMA": schema,
        "DATABRICKS_HTTP_PATH": http_path,
        "DATABRICKS_TOKEN": token,
    }

    profile, profile_vars = create_profile_vars_databricks(airflow_connection, catalog, schema)
    assert profile == "databricks_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_postgres(airflow_connection: Connection) -> None:
    host = "my-hostname.com"
    login = "my-user"
    schema = "jaffle_shop"
    password = "abcdef12345"
    database = "reporting"
    port = 5432
    airflow_connection.host = host
    airflow_connection.login = login
    airflow_connection.password = password
    airflow_connection.schema = database
    airflow_connection.port = port

    expected_profile_vars = {
        "POSTGRES_HOST": host,
        "POSTGRES_USER": login,
        "POSTGRES_PASSWORD": password,
        "POSTGRES_DATABASE": database,
        "POSTGRES_PORT": str(port),
        "POSTGRES_SCHEMA": schema,
    }

    profile, profile_vars = create_profile_vars_postgres(airflow_connection, database, schema)
    assert profile == "postgres_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_postgres_no_database(
    airflow_connection: Connection,
) -> None:
    """
    Test that create_profile_vars_postgres uses the schema as the database if no database is provided
    """
    # postgres variables
    host = "my-hostname.com"
    login = "my-user"
    database = "reporting"
    schema = "jaffle_shop"
    password = "abcdef12345"
    port = 5432
    airflow_connection.host = host
    airflow_connection.login = login
    airflow_connection.password = password
    airflow_connection.schema = database
    airflow_connection.port = port

    expected_profile_vars = {
        "POSTGRES_HOST": host,
        "POSTGRES_USER": login,
        "POSTGRES_PASSWORD": password,
        "POSTGRES_DATABASE": database,
        "POSTGRES_PORT": str(port),
        "POSTGRES_SCHEMA": schema,
    }

    profile, profile_vars = create_profile_vars_postgres(airflow_connection, None, schema)
    assert profile == "postgres_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_postgres_no_schema(
    airflow_schemaless_connection: Connection,
) -> None:
    with pytest.raises(ValueError):
        create_profile_vars_postgres(airflow_schemaless_connection, None, None)


def test_create_profile_vars_exasol(airflow_connection: Connection) -> None:
    host = "my-hostname.com"
    login = "my-user"
    schema = "jaffle_shop"
    password = "abcdef12345"
    port = 8563
    airflow_connection.host = host
    airflow_connection.login = login
    airflow_connection.password = password
    airflow_connection.schema = schema
    airflow_connection.port = port
    airflow_connection.extra_dejson = {"encryption": True, "compression": True}

    expected_profile_vars = {
        "EXASOL_HOST": host,
        "EXASOL_USER": login,
        "EXASOL_PASSWORD": password,
        "EXASOL_DATABASE": schema,
        "EXASOL_PORT": str(port),
        "EXASOL_SCHEMA": schema,
        "EXASOL_ENCRYPTION": True,
        "EXASOL_COMPRESSION": True,
        "EXASOL_PROTOCOL_VERSION": "V3",
        "EXASOL_SOCKET_TIMEOUT": "30",
        "EXASOL_CONNECTION_TIMEOUT": "30",
    }

    profile, profile_vars = create_profile_vars_exasol(airflow_connection, None, schema)
    assert profile == "exasol_profile"
    assert profile_vars == expected_profile_vars


@pytest.mark.parametrize(
    ["account", "region", "expected_account"],
    [
        ("xy12345", None, "xy12345"),
        ("xy12345", "eu-west-2", "xy12345.eu-west-2"),
        ("xy12345.eu-west-2", "eu-west-2", "xy12345.eu-west-2"),
        pytest.param(
            "xy12345.us-east-1",
            "eu-west-2",
            "xy12345.us-east-1",
            marks=pytest.mark.xfail,
        ),
    ],
    ids=[
        "no region given",
        "region given and not in account given",
        "region given but in account",
        "region given but different to region in account",
    ],
)
def test_get_snowflake_account(account: str, region: Optional[str], expected_account: str) -> None:
    assert get_snowflake_account(account, region) == expected_account


@pytest.mark.parametrize(
    "extras_are_prefixed",
    [True, False],
    ids=["extras have prefix", "extras don't have prefix"],
)
def test_create_profile_vars_snowflake(airflow_connection: Connection, extras_are_prefixed: bool) -> None:
    region = "us-east"
    warehouse = "warehouse"
    database = "db"
    role = "role"
    account = "account"
    user = "user"
    password = "abcde12345"
    schema = "jaffle_shop"
    airflow_connection.login = user
    airflow_connection.password = password
    if extras_are_prefixed:
        airflow_connection.extra_dejson = {
            "extra__snowflake__account": account,
            "extra__snowflake__region": region,
            "extra__snowflake__role": role,
            "extra__snowflake__warehouse": warehouse,
        }
    else:
        airflow_connection.extra_dejson = {
            "account": account,
            "region": region,
            "role": role,
            "warehouse": warehouse,
        }

    expected_profile_vars = {
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_ACCOUNT": f"{account}.{region}",
        "SNOWFLAKE_ROLE": role,
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_SCHEMA": schema,
    }

    profile, profile_vars = create_profile_vars_snowflake(airflow_connection, database, schema)
    assert profile == "snowflake_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_redshift(airflow_connection: Connection) -> None:
    host = "jaffle-shop.123456.eu-west-2.redshift.amazonaws.com"
    port = 5439
    login = "my-user"
    schema = "jaffle_shop"
    password = "abcdef12345"
    database = "reporting"
    airflow_connection.host = host
    airflow_connection.port = port
    airflow_connection.login = login
    airflow_connection.password = password

    expected_profile_vars = {
        "REDSHIFT_HOST": host,
        "REDSHIFT_PORT": str(port),
        "REDSHIFT_USER": login,
        "REDSHIFT_PASSWORD": password,
        "REDSHIFT_DATABASE": database,
        "REDSHIFT_SCHEMA": schema,
    }

    profile, profile_vars = create_profile_vars_redshift(airflow_connection, database, schema)
    assert profile == "redshift_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_redshift_no_schema(
    airflow_schemaless_connection: Connection,
) -> None:
    with pytest.raises(ValueError):
        create_profile_vars_redshift(airflow_schemaless_connection, None, None)


def test_create_profile_vars_google_cloud_platform(
    airflow_connection: Connection,
) -> None:
    database = "bigquery-project"
    schema = "jaffle_shop"
    private_key_id = "abcde12345"
    private_key = """
    -----BEGIN PRIVATE KEY -----
    asdjhjsNDFLKJASDFKMKLKNDFSNns
    -----END PRIVATE KEY -----
    """
    client_email = "dbt-admin@bigquery-project.iam.gserviceaccount.com"
    client_id = "1236590123789234"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/dbt-admin"

    bigquery_key_file_contents = json.dumps(
        {
            "type": "",
            "project_id": database,
            "private_key_id": private_key_id,
            "private_key": private_key,
            "client_email": client_email,
            "client_id": client_id,
            "auth_uri": auth_uri,
            "token_uri": token_uri,
            "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
            "client_x509_cert_url": client_x509_cert_url,
        }
    )
    airflow_connection.extra_dejson = {"keyfile_dict": bigquery_key_file_contents}

    expected_profile_vars = {
        "BIGQUERY_DATASET": schema,
        "BIGQUERY_PROJECT": database,
        "BIGQUERY_TYPE": "",
        "BIGQUERY_PROJECT_ID": database,
        "BIGQUERY_PRIVATE_KEY_ID": private_key_id,
        "BIGQUERY_PRIVATE_KEY": private_key,
        "BIGQUERY_CLIENT_EMAIL": client_email,
        "BIGQUERY_CLIENT_ID": client_id,
        "BIGQUERY_AUTH_URI": auth_uri,
        "BIGQUERY_TOKEN_URI": token_uri,
        "BIGQUERY_AUTH_PROVIDER_X509_CERT_URL": auth_provider_x509_cert_url,
        "BIGQUERY_CLIENT_X509_CERT_URL": client_x509_cert_url,
    }

    profile, profile_vars = create_profile_vars_google_cloud_platform(airflow_connection, database, schema)
    assert profile == "bigquery_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_trino(airflow_connection: Connection) -> None:
    host = "my-presto-host.com"
    port = 8443
    login = "my-user"
    schema = "jaffle_shop"
    password = "abcdef12345"
    catalog = "hive"
    http_scheme = "https"
    airflow_connection.host = host
    airflow_connection.port = port
    airflow_connection.login = login
    airflow_connection.password = password
    airflow_connection.extra_dejson = {
        "protocol": http_scheme,
        "catalog": catalog,
    }

    expected_profile_vars = {
        "TRINO_USER": login,
        "TRINO_PASSWORD": password,
        "TRINO_HTTP_SCHEME": http_scheme,
        "TRINO_DATABASE": "delta",
        "TRINO_SCHEMA": schema,
        "TRINO_HOST": host,
        "TRINO_PORT": str(port),
        "TRINO_AUTH_TYPE": "ldap",
    }

    profile, profile_vars = create_profile_vars_trino(airflow_connection, "delta", schema)
    assert profile == "trino_profile"
    assert profile_vars == expected_profile_vars


@patch("cosmos.providers.dbt.core.utils.profiles_generator.BaseHook")
@pytest.mark.parametrize(
    "fixture_name",
    [
        "snowflake_connection",
        "snowflake_extra_connection",
        "google_cloud_platform_connection",
        "redshift_connection",
        "postgres_connection",
        "databricks_connection",
        pytest.param("random_connection", marks=pytest.mark.xfail(raises=SystemExit)),
    ],
)
def test_map_profile(
    p_base_hook: MagicMock,
    request: pytest.FixtureRequest,
    fixture_name: str,
) -> None:
    connection: Connection = request.getfixturevalue(fixture_name)
    p_base_hook.return_value.get_connection.return_value = connection
    result = map_profile(fixture_name, "some_db", "some_schema")
    assert result is not None


def test_get_available_adapters():
    """
    To ensure consistency between the adapters. They should all take the same arguments regardless of whether they are
    used or not and should all return the same data structure.
    """
    adapters = get_available_adapters()
    for _, adapter_config in adapters.items():
        function_signature = inspect.signature(adapter_config.create_profile_function)
        assert len(function_signature.parameters) == 3
        assert function_signature.return_annotation == "tuple[str, dict[str, str]]"
