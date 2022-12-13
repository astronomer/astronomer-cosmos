import os
import sys

import yaml
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from cosmos.providers.dbt.core.profiles.postgres import postgres_profile
from cosmos.providers.dbt.core.profiles.snowflake import snowflake_profile


def create_default_profiles():
    profiles = {"postgres_profile": postgres_profile, "snowflake_profile": snowflake_profile}

    # Define the path to the directory and file
    home_dir = os.path.expanduser("~")
    directory_path = f"{home_dir}/.dbt"
    file_path = f"{home_dir}/.dbt/profiles.yml"

    # Create the directory if it does not exist
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Create the file if it does not exist
    if not os.path.exists(file_path):
        print("profiles.yml not found - initializing.")
        with open(file_path, "w") as file:
            yaml.dump(profiles, file)
        print("done")
    else:
        print("profiles.yml found - skipping")


def create_profile_vars(conn: Connection, schema_override):
    if conn.conn_type == "postgres":
        profile = "postgres_profile"
        profile_vars = {
            "POSTGRES_HOST": conn.host,
            "POSTGRES_USER": conn.login,
            "POSTGRES_PASSWORD": conn.password,
            "POSTGRES_DATABASE": conn.schema,
            "POSTGRES_PORT": str(conn.port),
            "POSTGRES_SCHEMA": schema_override,
        }

    elif conn.conn_type == "snowflake":
        profile = "snowflake_profile"
        profile_vars = {
            "SNOWFLAKE_USER": conn.login,
            "SNOWFLAKE_PASSWORD": conn.password,
            "SNOWFLAKE_ACCOUNT": f"{conn.extra_dejson.get('account')}.{conn.extra_dejson.get('region')}",
            "SNOWFLAKE_ROLE": conn.extra_dejson.get("role"),
            "SNOWFLAKE_DATABASE": conn.extra_dejson.get("database"),
            "SNOWFLAKE_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "SNOWFLAKE_SCHEMA": conn.schema,
        }

    else:
        print(f"Connection type {conn.type} is not yet supported.", file=sys.stderr)
        sys.exit(1)

    return profile, profile_vars


def map_profile(conn_id, schema):
    conn = BaseHook().get_connection(conn_id)
    profile, profile_vars = create_profile_vars(conn, schema_override=schema)
    return profile, profile_vars
