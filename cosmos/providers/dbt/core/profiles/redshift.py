from typing import TYPE_CHECKING, Optional, Tuple

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
    conn: "Connection",
    database_override: Optional[str] = None,
    schema_override: Optional[str] = None,
) -> Tuple[str, dict]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
    """
    schema_options = [schema_override, database_override, conn.schema]
    try:
        schema = next(schema for schema in schema_options if schema is not None)
    except StopIteration as e:
        msg = "A schema must be provided as either `db_name`, `schema` or in the schema field of the connection"
        raise ValueError(msg) from e

    profile_vars = {
        "REDSHIFT_HOST": conn.host,
        "REDSHIFT_PORT": str(conn.port),
        "REDSHIFT_USER": conn.login,
        "REDSHIFT_PASSWORD": conn.password,
        "REDSHIFT_DATABASE": schema,
        "REDSHIFT_SCHEMA": schema_override if schema_override else conn.schema,
    }
    return "redshift_profile", profile_vars
