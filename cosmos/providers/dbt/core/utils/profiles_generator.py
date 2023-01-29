import fcntl
import json
import logging
import os
import sys
from pathlib import Path

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


def create_profile_vars(conn: Connection, database, schema):
    if conn.conn_type == "postgres":
        profile = "postgres_profile"
        profile_vars = {
            "POSTGRES_HOST": conn.host,
            "POSTGRES_USER": conn.login,
            "POSTGRES_PASSWORD": conn.password,
            "POSTGRES_DATABASE": database,
            "POSTGRES_PORT": str(conn.port),
            "POSTGRES_SCHEMA": schema,
        }

    elif conn.conn_type == "snowflake":
        profile = "snowflake_profile"
        extras = {
            "account": "account",
            "region": "region",
            "role": "role",
            "warehouse": "warehouse",
        }

        # At some point the extras removed a prefix extra__snowflake__ when the provider got updated... handling that
        # here.
        if conn.extra_dejson.get("account") is None:
            for key, value in extras.items():
                extras[key] = f"extra__snowflake__{value}"

        # Region is optional
        region = conn.extra_dejson.get(extras["region"])
        account = conn.extra_dejson.get(extras["account"])

        if region and region not in account:
            account = f"{conn.extra_dejson.get(extras['account'])}.{region}"
        else:
            account = conn.extra_dejson.get(extras["account"])

        profile_vars = {
            "SNOWFLAKE_USER": conn.login,
            "SNOWFLAKE_PASSWORD": conn.password,
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_ROLE": conn.extra_dejson.get(extras["role"]),
            "SNOWFLAKE_DATABASE": database,
            "SNOWFLAKE_WAREHOUSE": conn.extra_dejson.get(extras["warehouse"]),
            "SNOWFLAKE_SCHEMA": schema,
        }

    elif conn.conn_type == "redshift":
        profile = "redshift_profile"
        profile_vars = {
            "REDSHIFT_HOST": conn.host,
            "REDSHIFT_PORT": str(conn.port),
            "REDSHIFT_USER": conn.login,
            "REDSHIFT_PASSWORD": conn.password,
            "REDSHIFT_DATABASE": database,
            "REDSHIFT_SCHEMA": schema,
        }

    elif conn.conn_type == "google_cloud_platform":
        profile = "bigquery_profile"
        profile_vars = {
            "BIGQUERY_DATASET": schema,
            "BIGQUERY_PROJECT": database,
            "BIGQUERY_TYPE": json.loads(conn.extra_dejson.get("keyfile_dict"))["type"],
            "BIGQUERY_PROJECT_ID": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "project_id"
            ],
            "BIGQUERY_PRIVATE_KEY_ID": json.loads(
                conn.extra_dejson.get("keyfile_dict")
            )["private_key_id"],
            "BIGQUERY_PRIVATE_KEY": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "private_key"
            ],
            "BIGQUERY_CLIENT_EMAIL": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "client_email"
            ],
            "BIGQUERY_CLIENT_ID": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "client_id"
            ],
            "BIGQUERY_AUTH_URI": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "auth_uri"
            ],
            "BIGQUERY_TOKEN_URI": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "token_uri"
            ],
            "BIGQUERY_AUTH_PROVIDER_X509_CERT_URL": json.loads(
                conn.extra_dejson.get("keyfile_dict")
            )["auth_provider_x509_cert_url"],
            "BIGQUERY_CLIENT_X509_CERT_URL": json.loads(
                conn.extra_dejson.get("keyfile_dict")
            )["client_x509_cert_url"],
        }

    elif conn.conn_type == "databricks":
        profile = "databricks_profile"
        profile_vars = {
            "DATABRICKS_HOST": conn.host,
            "DATABRICKS_SCHEMA": schema,
            "DATABRICKS_HTTP_PATH": conn.extra_dejson.get("http_path"),
            "DATABRICKS_TOKEN": conn.extra_dejson.get("token"),
        }

    else:
        logger.error(
            f"Connection type {conn.type} is not yet supported.", file=sys.stderr
        )
        sys.exit(1)

    return profile, profile_vars


def map_profile(conn_id, db_override=None, schema_override=None):
    conn = BaseHook().get_connection(conn_id)

    """
    A really annoying edge case: snowflake uses the schema field of a connection as an actual schema where every other
    connection object (bigquery, redshift, postgres) uses the schema field to specify the database. So this logic should
    handle that and allow users to either specify the target schema in the snowflake connection
    """

    # set db override
    if db_override:
        db = db_override
    else:
        if conn.conn_type != "snowflake":
            if conn.conn_type == "google_cloud_platform":
                db = json.loads(conn.extra_dejson.get("keyfile_dict"))["project_id"]
            else:
                db = conn.schema
        else:
            # At some point the extras removed a prefix extra__snowflake__ when the provider got updated... handling
            # that here.
            if conn.extra_dejson.get("database") is not None:
                db = conn.extra_dejson.get("database")
            else:
                db = conn.extra_dejson.get("extra__snowflake__database")

    # set schema override
    if conn.conn_type == "snowflake":
        if conn.schema is not None and schema_override is not None:
            schema = schema_override
        elif schema_override is not None:
            schema = schema_override
        elif conn.schema is not None and schema_override is None:
            schema = conn.schema
        else:
            logging.getLogger().setLevel(logging.ERROR)
            logging.error(
                f"Please specify a target schema in the {conn.id} connection properties or using the schema parameter."
            )
            sys.exit(1)
    else:
        if schema_override is not None:
            schema = schema_override
        else:
            logging.getLogger().setLevel(logging.ERROR)
            logging.error("Please specify a target schema using the schema parameter.")
            sys.exit(1)

    profile, profile_vars = create_profile_vars(conn, database=db, schema=schema)
    return profile, profile_vars
