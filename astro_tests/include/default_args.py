import os

if os.getenv("ENVIRONMENT", "DEV") == "PROD":
    sources = [
        {"conn_type": "snowflake", "conn_id": "snowflake_default", "schema": "chronek"},
        {
            "conn_type": "bigquery",
            "conn_id": "bigquery_default",
            "schema": "cosmos_testing",
        },
        {"conn_type": "postgres", "conn_id": "postgres_default", "schema": "public"},
        {"conn_type": "redshift", "conn_id": "redshift_default", "schema": "public"},
        {
            "conn_type": "databricks",
            "conn_id": "databricks_default",
            "schema": "cosmos",
        },
    ]
else:
    sources = [{"conn_type": "postgres", "conn_id": "airflow_db", "schema": "public"}]
