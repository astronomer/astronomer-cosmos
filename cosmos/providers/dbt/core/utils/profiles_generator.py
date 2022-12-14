import json
import logging
import os
import sys

import yaml
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles.bigquery import bigquery_profile
from cosmos.providers.dbt.core.profiles.postgres import postgres_profile
from cosmos.providers.dbt.core.profiles.redshift import redshift_profile
from cosmos.providers.dbt.core.profiles.snowflake import snowflake_profile

logger = logging.getLogger(__name__)


def create_default_profiles():
    """
    Generates a default profiles.yml file if one does not exist.

    :return: The path to the profiles.yml file
    :rtype: str
    """

    profiles = {
        "postgres_profile": postgres_profile,
        "snowflake_profile": snowflake_profile,
        "redshift_profile": redshift_profile,
        "bigquery_profile": bigquery_profile,
    }

    # Define the path to the directory and file
    home_dir = os.path.expanduser("~")
    directory_path = f"{home_dir}/.dbt"
    file_path = f"{home_dir}/.dbt/profiles.yml"

    # Create the directory if it does not exist
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Create the file if it does not exist
    if not os.path.exists(file_path):
        logging.info("profiles.yml not found - initializing.")
        with open(file_path, "w") as file:
            yaml.dump(profiles, file)
        logging.info("done")
    else:
        logging.info("profiles.yml found - skipping")

    return file_path


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
        profile_vars = {
            "SNOWFLAKE_USER": conn.login,
            "SNOWFLAKE_PASSWORD": conn.password,
            "SNOWFLAKE_ACCOUNT": f"{conn.extra_dejson.get('account')}.{conn.extra_dejson.get('region')}",
            "SNOWFLAKE_ROLE": conn.extra_dejson.get("role"),
            "SNOWFLAKE_DATABASE": database,
            "SNOWFLAKE_WAREHOUSE": conn.extra_dejson.get("warehouse"),
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
            "BIGQUERY_PROJECT": json.loads(conn.extra_dejson.get("keyfile_dict"))["project_id"],
            "BIGQUERY_TYPE": json.loads(conn.extra_dejson.get("keyfile_dict"))["type"],
            "BIGQUERY_PROJECT_ID": json.loads(conn.extra_dejson.get("keyfile_dict"))["project_id"],
            "BIGQUERY_PRIVATE_KEY_ID": json.loads(conn.extra_dejson.get("keyfile_dict"))["private_key_id"],
            "BIGQUERY_PRIVATE_KEY": json.loads(conn.extra_dejson.get("keyfile_dict"))["private_key"],
            "BIGQUERY_CLIENT_EMAIL": json.loads(conn.extra_dejson.get("keyfile_dict"))["client_email"],
            "BIGQUERY_CLIENT_ID": json.loads(conn.extra_dejson.get("keyfile_dict"))["client_id"],
            "BIGQUERY_AUTH_URI": json.loads(conn.extra_dejson.get("keyfile_dict"))["auth_uri"],
            "BIGQUERY_TOKEN_URI": json.loads(conn.extra_dejson.get("keyfile_dict"))["token_uri"],
            "BIGQUERY_AUTH_PROVIDER_X509_CERT_URL": json.loads(conn.extra_dejson.get("keyfile_dict"))[
                "auth_provider_x509_cert_url"
            ],
            "BIGQUERY_CLIENT_X509_CERT_URL": json.loads(conn.extra_dejson.get("keyfile_dict"))["client_x509_cert_url"],
        }

    else:
        logging.error(f"Connection type {conn.type} is not yet supported.")
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
            db = conn.schema
        else:
            db = conn.extra_dejson.get("database")

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
