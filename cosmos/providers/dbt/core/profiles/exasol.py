from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

exasol_profile = {
    "outputs": {
        "dev": {
            "type": "exasol",
            "dsn": "{{ env_var('EXASOL_HOST') }}:{{ env_var('EXASOL_PORT') | as_number }}",
            "user": "{{ env_var('EXASOL_USER') }}",
            "password": "{{ env_var('EXASOL_PASSWORD') }}",
            "dbname": "{{ env_var('EXASOL_DATABASE') }}",
            "schema": "{{ env_var('EXASOL_SCHEMA') }}",
            "encryption": "{{ env_var('EXASOL_ENCRYPTION') }}",
            "compression": "{{ env_var('EXASOL_COMPRESSION') }}",
            "connect_timeout": "{{ env_var('EXASOL_CONNECT_TIMEOUT') }}",
            "socket_timeout": "{{ env_var('EXASOL_SOCKET_TIMEOUT') }}",
            "protocol_version": "{{ env_var('EXASOL_PROTOCOL_VERSION') }}",
        }
    },
    "target": "dev",
}


def create_profile_vars_exasol(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/exasol-setup
    https://airflow.apache.org/docs/apache-airflow-providers-exasol/stable/index.html
    """

    if not schema_override:
        raise ValueError("A exasol schema must be provided via the `schema` parameter")

    extras = conn.extra_dejson

    profile_vars = {
        "EXASOL_HOST": conn.host,
        "EXASOL_USER": conn.login,
        "EXASOL_PASSWORD": conn.password,
        # airflow uses schema connection field for db - except Snowflake
        "EXASOL_PORT": str(conn.port),
        "EXASOL_DATABASE": database_override if database_override else conn.schema,
        "EXASOL_SCHEMA": schema_override,
        "EXASOL_ENCRYPTION": extras.get("encryption", "false"),
        "EXASOL_COMPRESSION": extras.get("compression", "false"),
        "EXASOL_CONNECTION_TIMEOUT": extras.get("connection_timeout", "30"),
        "EXASOL_SOCKET_TIMEOUT": extras.get("socket_timeout", "30"),
        "EXASOL_PROTOCOL_VERSION": extras.get("protocol_version", "V3"),
    }
    return "exasol_profile", profile_vars
