from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

postgres_profile = {
    "outputs": {
        "dev": {
            "type": "postgres",
            "host": "{{ env_var('POSTGRES_HOST') }}",
            "port": "{{ env_var('POSTGRES_PORT') | as_number }}",
            "user": "{{ env_var('POSTGRES_USER') }}",
            "pass": "{{ env_var('POSTGRES_PASSWORD') }}",
            "dbname": "{{ env_var('POSTGRES_DATABASE') }}",
            "schema": "{{ env_var('POSTGRES_SCHEMA') }}",
        }
    },
    "target": "dev",
}


def create_profile_vars_postgres(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
    https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html
    """

    if not schema_override:
        raise ValueError("A postgres schema must be provided via the `schema` parameter")

    profile_vars = {
        "POSTGRES_HOST": conn.host,
        "POSTGRES_USER": conn.login,
        "POSTGRES_PASSWORD": conn.password,
        # airflow uses schema connection field for db - except Snowflake
        "POSTGRES_DATABASE": database_override if database_override else conn.schema,
        "POSTGRES_PORT": str(conn.port),
        "POSTGRES_SCHEMA": schema_override,
    }
    return "postgres_profile", profile_vars
