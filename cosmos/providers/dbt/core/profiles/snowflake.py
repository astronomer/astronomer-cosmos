from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

snowflake_profile = {
    "target": "dev",
    "outputs": {
        "dev": {
            "type": "snowflake",
            "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
            "user": "{{ env_var('SNOWFLAKE_USER') }}",
            "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
            "role": "{{ env_var('SNOWFLAKE_ROLE') }}",
            "database": "{{ env_var('SNOWFLAKE_DATABASE') }}",
            "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE') }}",
            "schema": "{{ env_var('SNOWFLAKE_SCHEMA') }}",
            "client_session_keep_alive": False,
        }
    },
}


def get_snowflake_account(account: str, region: str | None = None) -> str:
    # Region is optional
    if region and region not in account:
        account = f"{account}.{region}"
    return account


def create_profile_vars_snowflake(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """
    extras = {
        "account": "account",
        "database": "database",
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
    account = get_snowflake_account(snowflake_dejson.get(extras["account"]), snowflake_dejson.get(extras["region"]))
    profile_vars = {
        "SNOWFLAKE_USER": conn.login,
        "SNOWFLAKE_PASSWORD": conn.password,
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_ROLE": snowflake_dejson.get(extras["role"]),
        "SNOWFLAKE_DATABASE": database_override if database_override else conn.extra_dejson.get(extras["database"]),
        "SNOWFLAKE_WAREHOUSE": snowflake_dejson.get(extras["warehouse"]),
        "SNOWFLAKE_SCHEMA": schema_override if schema_override else conn.schema,
    }
    return "snowflake_profile", profile_vars
