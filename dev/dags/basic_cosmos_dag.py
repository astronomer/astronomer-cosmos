"""
An example DAG that uses Cosmos to render a dbt project.
"""
import os
from datetime import datetime

from cosmos.providers.dbt.dag import DbtDag

DBT_ROOT_PATH = os.getenv("DBT_ROOT_PATH", "/usr/local/airflow/dags/dbt")

basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    dbt_root_path=DBT_ROOT_PATH,
    dbt_project_name="jaffle_shop",
    conn_id="airflow_db",
    dbt_args={"schema": "public"},
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dag",
)
