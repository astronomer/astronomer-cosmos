from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

logger = logging.getLogger(__name__)

# spark dbt profile
spark_profile = {
    "outputs": {
        "dev": {
            "type": "spark",
            "host": "{{ env_var('SPARK_HOST') }}",
            "method": "{{ env_var('SPARK_METHOD', 'thrift') }}",
            "port": "{{ env_var('SPARK_PORT') | as_number }}",
            "schema": "{{ env_var('SPARK_SCHEMA') }}",
            "user": "{{ env_var('SPARK_USER') }}",
        }
    },
    "target": "dev",
}


# Create a profile for spark thrift
def create_profile_vars_spark_thrift(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-profiles/spark-profile
    https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html
    """
    if schema_override:
        schema = schema_override
    elif conn.schema:
        schema = conn.schema
    else:
        schema = "default"
        logging.info(f"Using schema: {schema} as default since none specified in db_name or connection schema")

    profile_vars = {
        "SPARK_HOST": conn.host,
        "SPARK_METHOD": "thrift",
        "SPARK_PORT": str(conn.port),
        "SPARK_SCHEMA": schema,
    }

    return "spark_profile", profile_vars
