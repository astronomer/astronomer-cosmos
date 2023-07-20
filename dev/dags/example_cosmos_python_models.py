"""
An example DAG that uses Cosmos to render a dbt project which uses Python models (Databricks).

It relies on the user setting the connection databricks_default.

This can be done via environment variable:

export AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://@dbc-<account-id>.cloud.databricks.com?token=<access-token>&http_path=/sql/1.0/warehouses/<warehouse-id>

Replacing:
- <account-id>: Databricks account ID
- <access-token>: Databricks access token
- <warehouse-id>: Databricks SQL Warehouse ID from connection HTTP path
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
SCHEMA = "cosmos_" + os.getenv("DATABRICKS_UNIQUE_ID", "")

# [START example_cosmos_python_models]
example_cosmos_python_models = DbtDag(
    # dbt/cosmos-specific parameters
    dbt_root_path=DBT_ROOT_PATH,
    dbt_project_name="jaffle_shop_python",
    conn_id="databricks_default",
    profile_args={
        "schema": "cosmos",
    },
    operator_args={"append_env": True},
    profile_name_override="airflow",
    target_name_override="dev_target",
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_cosmos_python_models",
)
# [END example_cosmos_python_models]
