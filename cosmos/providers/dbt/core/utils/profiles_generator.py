import fcntl
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import pkg_resources
import yaml
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles.bigquery import bigquery_profile
from cosmos.providers.dbt.core.profiles.databricks import databricks_profile
from cosmos.providers.dbt.core.profiles.postgres import postgres_profile
from cosmos.providers.dbt.core.profiles.redshift import redshift_profile
from cosmos.providers.dbt.core.profiles.snowflake import snowflake_profile

logger = logging.getLogger(__name__)


def create_default_profiles():
    # get installed version of astronomer-cosmos
    try:
        package = pkg_resources.get_distribution("astronomer-cosmos")
    except pkg_resources.DistributionNotFound:
        package = None

    profiles = {
        "postgres_profile": postgres_profile,
        "snowflake_profile": snowflake_profile,
        "redshift_profile": redshift_profile,
        "bigquery_profile": bigquery_profile,
        "databricks_profile": databricks_profile,
    }

    # Define the path to the directory and file
    home_dir = os.path.expanduser("~")
    file_path = f"{home_dir}/.dbt/profiles.yml"

    # Create the file if it does not exist
    profile_file = Path(file_path)

    if profile_file.exists():
        # check the version of cosmos when it was created
        with open(file_path) as f:
            first_line = next(f)
        if first_line != f"# {package}\n":
            # if version of cosmos has been updated - re-write the profiles.yml file
            with open(file_path, "w") as file:
                fcntl.flock(file, fcntl.LOCK_SH)
                file.write(f"# {package}\n")
                yaml.dump(profiles, file)
                fcntl.flock(file, fcntl.LOCK_UN)
    else:
        # make the parent dir
        profile_file.parent.mkdir(parents=True, exist_ok=True)

        # if file doesn't exist - write the profiles.yml file
        with open(file_path, "w") as file:
            fcntl.flock(file, fcntl.LOCK_SH)
            file.write(f"# {package}\n")
            yaml.dump(profiles, file)
            fcntl.flock(file, fcntl.LOCK_UN)


def create_profile_vars_postgres(
    conn: Connection, database: str, schema: str
) -> Tuple[str, dict]:
    profile_vars = {
        "POSTGRES_HOST": conn.host,
        "POSTGRES_USER": conn.login,
        "POSTGRES_PASSWORD": conn.password,
        "POSTGRES_DATABASE": database,
        "POSTGRES_PORT": str(conn.port),
        "POSTGRES_SCHEMA": schema,
    }
    return "postgres_profile", profile_vars


def get_snowflake_account(account: str, region: Optional[str] = None) -> str:
    # Region is optional
    if region and region not in account:
        account = f"{account}.{region}"
    return account


def create_profile_vars_snowflake(
    conn: Connection, database: str, schema: str
) -> Tuple[str, dict]:
    extras = {
        "account": "account",
        "region": "region",
        "role": "role",
        "warehouse": "warehouse",
    }
    snowflake_dejson = conn.extra_dejson
    # At some point the extras removed a prefix extra__snowflake__ when the provider got updated... handling that
    # here.
    if snowflake_dejson.get("account") is None:
        for key, value in extras.items():
            extras[key] = f"extra__snowflake__{value}"
    account = get_snowflake_account(
        snowflake_dejson.get(extras["account"]), snowflake_dejson.get(extras["region"])
    )
    profile_vars = {
        "SNOWFLAKE_USER": conn.login,
        "SNOWFLAKE_PASSWORD": conn.password,
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_ROLE": snowflake_dejson.get(extras["role"]),
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_WAREHOUSE": snowflake_dejson.get(extras["warehouse"]),
        "SNOWFLAKE_SCHEMA": schema,
    }
    return "snowflake_profile", profile_vars


def create_profile_vars_redshift(
    conn: Connection, database: str, schema: str
) -> Tuple[str, dict]:
    profile_vars = {
        "REDSHIFT_HOST": conn.host,
        "REDSHIFT_PORT": str(conn.port),
        "REDSHIFT_USER": conn.login,
        "REDSHIFT_PASSWORD": conn.password,
        "REDSHIFT_DATABASE": database,
        "REDSHIFT_SCHEMA": schema,
    }
    return "redshift_profile", profile_vars


def create_profile_vars_google_cloud_platform(
    conn: Connection, database: str, schema: str
) -> Tuple[str, dict]:
    bigquery_key_file = json.loads(conn.extra_dejson.get("keyfile_dict"))
    profile_vars = {
        "BIGQUERY_DATASET": schema,
        "BIGQUERY_PROJECT": database,
        "BIGQUERY_TYPE": bigquery_key_file["type"],
        "BIGQUERY_PROJECT_ID": bigquery_key_file["project_id"],
        "BIGQUERY_PRIVATE_KEY_ID": bigquery_key_file["private_key_id"],
        "BIGQUERY_PRIVATE_KEY": bigquery_key_file["private_key"],
        "BIGQUERY_CLIENT_EMAIL": bigquery_key_file["client_email"],
        "BIGQUERY_CLIENT_ID": bigquery_key_file["client_id"],
        "BIGQUERY_AUTH_URI": bigquery_key_file["auth_uri"],
        "BIGQUERY_TOKEN_URI": bigquery_key_file["token_uri"],
        "BIGQUERY_AUTH_PROVIDER_X509_CERT_URL": bigquery_key_file[
            "auth_provider_x509_cert_url"
        ],
        "BIGQUERY_CLIENT_X509_CERT_URL": bigquery_key_file["client_x509_cert_url"],
    }
    return "bigquery_profile", profile_vars


def create_profile_vars_databricks(
    conn: Connection, database: str, schema: str
) -> Tuple[str, dict]:

    # airflow throws warning messages if your token is in extras
    if conn.password:
        token = conn.password
    else:
        token = conn.extra_dejson.get("token")

    profile_vars = {
        "DATABRICKS_HOST": str(conn.host).replace(
            "https://", ""
        ),  # airflow doesn't mind if this is there, but dbt does
        "DATABRICKS_CATALOG": database,
        "DATABRICKS_DATABASE": schema,
        "DATABRICKS_HTTP_PATH": conn.extra_dejson.get("http_path"),
        "DATABRICKS_TOKEN": token,
    }
    return "databricks_profile", profile_vars


def get_db_from_connection(connection_type: str, conn: Connection) -> str:
    if connection_type == "snowflake":
        # At some point the extras removed a prefix extra__snowflake__ when the provider got updated... handling
        # that here.
        db = conn.extra_dejson.get(
            "database", conn.extra_dejson.get("extra__snowflake__database")
        )
    elif connection_type == "google_cloud_platform":
        db = json.loads(conn.extra_dejson.get("keyfile_dict"))["project_id"]
    else:
        db = conn.schema

    return db


def map_profile(
    conn_id: str,
    db_override: Optional[str] = None,
    schema_override: Optional[str] = None,
) -> Tuple[str, dict]:
    """
    A really annoying edge case: snowflake uses the schema field of a connection as an actual schema where every other
    connection object (bigquery, redshift, postgres) uses the schema field to specify the database. So this logic should
    handle that and allow users to either specify the target schema in the snowflake connection
    """
    conn = BaseHook().get_connection(conn_id)
    connection_type = conn.conn_type
    profile_vars_dispatch = {
        "postgres": create_profile_vars_postgres,
        "redshift": create_profile_vars_redshift,
        "snowflake": create_profile_vars_snowflake,
        "google_cloud_platform": create_profile_vars_google_cloud_platform,
        "databricks": create_profile_vars_databricks,
    }
    profile_vars_func = profile_vars_dispatch.get(connection_type)
    if not profile_vars_func:
        logging.getLogger().setLevel(logging.ERROR)
        logging.error(
            f"This connection type is currently not supported {connection_type}."
        )
        sys.exit(1)

    if db_override:
        # if there's a db provided as a parameter, use it first
        db = db_override
    else:
        # otherwise get it from the connection schema
        db = get_db_from_connection(connection_type, conn)

    ## if you weren't provided a db from the connection or as a parameter - get angry
    if not db:
        logging.getLogger().setLevel(logging.ERROR)
        logging.error(
            "Please specify a database in connection properties or using the db override parameter"
        )
        sys.exit(1)

    # if there's a schema provided as a parameter, use it first
    if schema_override:
        schema = schema_override

    # snowflake is the only connector where schema is a schema and not a database
    elif connection_type == "snowflake":
        schema = conn.schema
        # if you haven't specified the schema in the connection or as a parameter - get angry
        if not schema:
            logging.getLogger().setLevel(logging.ERROR)
            logging.error(
                f"Please specify a target schema in the {conn.id} connection properties or using the schema parameter."
            )
            sys.exit(1)

    else:
        # if you haven't specified the schema as a parameter - get angry
        logging.getLogger().setLevel(logging.ERROR)
        logging.error("Please specify a target schema using the schema parameter.")
        sys.exit(1)

    return profile_vars_func(conn, database=db, schema=schema)
