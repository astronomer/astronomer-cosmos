from typing import TYPE_CHECKING, Optional, Tuple

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
    conn: "Connection",
    database_override: Optional[str] = None,
    schema_override: Optional[str] = None,
) -> Tuple[str, dict]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
    https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html
    """
    schema_options = [schema_override, database_override, conn.schema]
    try:
        schema = next(schema for schema in schema_options if schema is not None)
    except StopIteration as e:
        msg = "A schema must be provided as either `db_name`, `schema` or in the schema field of the connection"
        raise ValueError(msg) from e
    profile_vars = {
        "POSTGRES_HOST": conn.host,
        "POSTGRES_USER": conn.login,
        "POSTGRES_PASSWORD": conn.password,
        "POSTGRES_DATABASE": schema,
        "POSTGRES_PORT": str(conn.port),
        "POSTGRES_SCHEMA": schema,  # TODO can we remove this? it's unused...
    }
    return "postgres_profile", profile_vars
