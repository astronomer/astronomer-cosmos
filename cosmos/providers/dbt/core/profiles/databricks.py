from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

logger = logging.getLogger(__name__)

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
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html

    Database override is used to reference a Unity Catalog which was made available in dbt-databricks>=1.1.1
    Airflow recommends specifying token in the password field as it's more secure.
    If the host contains the https then we remove it.
    """
    if conn.password:
        token = conn.password
    else:
        token = conn.extra_dejson.get("token")

    if not schema_override:
        raise ValueError("A databricks database must be provided via the `schema` parameter")

    if database_override:
        catalog = database_override
    elif conn.schema:
        catalog = conn.schema
    else:
        # see https://docs.databricks.com/data-governance/unity-catalog/hive-metastore.html#default-catalog
        catalog = "hive_metastore"
        logging.info(f"Using catalog: {catalog} as default since none specified in db_name or connection schema")

    profile_vars = {
        "DATABRICKS_HOST": str(conn.host).replace("https://", ""),
        "DATABRICKS_CATALOG": catalog,
        "DATABRICKS_SCHEMA": schema_override,
        "DATABRICKS_HTTP_PATH": conn.extra_dejson.get("http_path"),
        "DATABRICKS_TOKEN": token,
    }
    return "databricks_profile", profile_vars
