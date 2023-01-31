import json
from typing import Generator, Optional
from unittest.mock import MagicMock, patch

import pytest
from airflow.models.connection import Connection

from cosmos.providers.dbt.core.utils.profiles_generator import (
    create_profile_vars_databricks,
    create_profile_vars_google_cloud_platform,
    create_profile_vars_postgres,
    create_profile_vars_redshift,
    create_profile_vars_snowflake,
    get_db_from_connection,
    get_snowflake_account,
    map_profile,
)


@pytest.fixture()
def airflow_connection() -> Generator[Connection, None, None]:
    yield MagicMock(spec=Connection)


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
def postgres_schemaless_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "postgres"
    airflow_connection.schema = None
    yield airflow_connection


@pytest.fixture()
def databricks_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "databricks"
    airflow_connection.schema = "test_database"
    yield airflow_connection


@pytest.fixture()
def databricks_schemaless_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "databricks"
    airflow_connection.schema = None
    yield airflow_connection


@pytest.fixture()
def random_connection(
    airflow_connection: Connection,
) -> Generator[Connection, None, None]:
    airflow_connection.conn_type = "random"
    airflow_connection.schema = "test_database"
    yield airflow_connection


def test_create_profile_vars_databricks(
    airflow_connection: Generator[Connection, None, None]
) -> None:
    host = 1234
    schema = "jaffle_shop"
    http_path = "sql/protocolv1/o/1234567891234567/1234-123456-a1bc2d3e"
    token = "dapiab123456cdefgh78910"
    airflow_connection.host = host
    airflow_connection.schema = schema
    airflow_connection.extra_dejson = {"http_path": http_path, "token": token}

    expected_profile_vars = {
        "DATABRICKS_HOST": host,
        "DATABRICKS_SCHEMA": schema,
        "DATABRICKS_HTTP_PATH": http_path,
        "DATABRICKS_TOKEN": token,
    }

    profile, profile_vars = create_profile_vars_databricks(
        airflow_connection, "reporting", schema
    )
    assert profile == "databricks_profile"
    assert profile_vars == expected_profile_vars


def test_create_profile_vars_postgres(airflow_connection: Connection) -> None:
    host = 1234
    login = "my-user"
    schema = "jaffle_shop"
    password = "abcdef12345"
    database = "reporting"
    port = 5432
    airflow_connection.host = host
    airflow_connection.login = login
    airflow_connection.password = password
    airflow_connection.port = port

    expected_profile_vars = {
        "POSTGRES_HOST": host,
        "POSTGRES_USER": login,
        "POSTGRES_PASSWORD": password,
        "POSTGRES_DATABASE": database,
        "POSTGRES_PORT": str(port),
        "POSTGRES_SCHEMA": schema,
    }

    profile, profile_vars = create_profile_vars_postgres(
        airflow_connection, database, schema
    )
    assert profile == "postgres_profile"
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
def test_get_snowflake_account(
    account: str, region: Optional[str], expected_account: str
) -> None:
    assert get_snowflake_account(account, region) == expected_account


@pytest.mark.parametrize(
    "extras_are_prefixed",
    [True, False],
    ids=["extras have prefix", "extras don't have prefix"],
)
def test_create_profile_vars_snowflake(
    airflow_connection: Connection, extras_are_prefixed: bool
) -> None:
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

    profile, profile_vars = create_profile_vars_snowflake(
        airflow_connection, database, schema
    )
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

    profile, profile_vars = create_profile_vars_redshift(
        airflow_connection, database, schema
    )
    assert profile == "redshift_profile"
    assert profile_vars == expected_profile_vars


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

    profile, profile_vars = create_profile_vars_google_cloud_platform(
        airflow_connection, database, schema
    )
    assert profile == "bigquery_profile"
    assert profile_vars == expected_profile_vars


@pytest.mark.parametrize(
    "fixture_name",
    [
        "snowflake_connection",
        "snowflake_extra_connection",
        "google_cloud_platform_connection",
        pytest.param("redshift_connection", marks=pytest.mark.xfail),
        "postgres_connection",
        "databricks_connection",
    ],
)
def test_get_db_from_connection(
    fixture_name: str,
    request: pytest.FixtureRequest,
):
    connection: Connection = request.getfixturevalue(fixture_name)
    database = get_db_from_connection(connection.conn_type, connection)
    assert database == "test_database"


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


@patch("cosmos.providers.dbt.core.utils.profiles_generator.BaseHook")
@pytest.mark.parametrize(
    "fixture_name",
    [
        "postgres_schemaless_connection",
        "databricks_schemaless_connection",
    ],
)
def test_map_profile_no_schema(
    p_base_hook: MagicMock, request: pytest.FixtureRequest, fixture_name: str
):
    connection: Connection = request.getfixturevalue(fixture_name)
    p_base_hook.return_value.get_connection.return_value = connection
    with pytest.raises(SystemExit):
        map_profile("my_connection")


@patch("cosmos.providers.dbt.core.utils.profiles_generator.BaseHook")
def test_map_profile_no_db(
    p_base_hook: MagicMock,
    postgres_schemaless_connection: Connection,
) -> None:
    p_base_hook.return_value.get_connection.return_value = (
        postgres_schemaless_connection
    )
    with pytest.raises(SystemExit):
        map_profile("my_connection", schema_override="jaffle_shop")
