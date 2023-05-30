from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

redshift_profile = {
    "outputs": {
        "dev": {
            "type": "redshift",
            "host": "{{ env_var('REDSHIFT_HOST') }}",
            "port": "{{ env_var('REDSHIFT_PORT') | as_number }}",
            "user": "{{ env_var('REDSHIFT_USER') }}",
            "password": "{{ env_var('REDSHIFT_PASSWORD') }}",
            "dbname": "{{ env_var('REDSHIFT_DATABASE') }}",
            "schema": "{{ env_var('REDSHIFT_SCHEMA') }}",
            "ra3_node": True,
        }
    },
    "target": "dev",
}


def create_profile_vars_redshift(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
    """

    if not schema_override:
        raise ValueError("A redshift schema must be provided via the `schema` parameter")

    profile_vars = {
        "REDSHIFT_HOST": conn.host,
        "REDSHIFT_PORT": str(conn.port),
        "REDSHIFT_USER": conn.login,
        "REDSHIFT_PASSWORD": conn.password,
        # airflow uses schema connection field for db - except Snowflake
        "REDSHIFT_DATABASE": database_override if database_override else conn.schema,
        "REDSHIFT_SCHEMA": schema_override,
    }
    return "redshift_profile", profile_vars
