from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    from airflow.models import Connection

databricks_profile = {
    "outputs": {
        "dev": {
            "type": "databricks",
            "host": "{{ env_var('DATABRICKS_HOST') }}",
            "catalog": "{{ env_var('DATABRICKS_CATALOG') }}",
            "schema": "{{ env_var('DATABRICKS_SCHEMA') }}",
            "http_path": "{{ env_var('DATABRICKS_HTTP_PATH') }}",
            "token": "{{ env_var('DATABRICKS_TOKEN') }}",
        }
    },
    "target": "dev",
}


def create_profile_vars_databricks(
    conn: "Connection",
    database_override: Optional[str] = None,
    schema_override: Optional[str] = None,
) -> Tuple[str, dict]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html

    Database override is used to reference a Unity Catalog which was made available in dbt-databricks>=1.1.1
    Airflow recommends specifying token in the password field as it's more secure.
    If the host contains the scheme then we remove it.
    """
    if conn.password:
        token = conn.password
    else:
        token = conn.extra_dejson.get("token")
    profile_vars = {
        "DATABRICKS_HOST": str(conn.host).replace("https://", ""),
        "DATABRICKS_CATALOG": database_override if database_override else "",
        "DATABRICKS_SCHEMA": schema_override if schema_override else conn.schema,
        "DATABRICKS_HTTP_PATH": conn.extra_dejson.get("http_path"),
        "DATABRICKS_TOKEN": token,
    }
    return "databricks_profile", profile_vars
