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

from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import DatabricksTokenProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
SCHEMA = "cosmos_" + os.getenv("DATABRICKS_UNIQUE_ID", "").replace(".", "_")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={"schema": SCHEMA, "connect_retries": 3},
    ),
)

# [START example_cosmos_python_models]
example_cosmos_python_models = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop_python",
    ),
    profile_config=profile_config,
    operator_args={
        "append_env": True,
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_cosmos_python_models",
    default_args={"retries": 0},
)
# [END example_cosmos_python_models]
