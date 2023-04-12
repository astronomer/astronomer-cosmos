"""
An example DAG that uses Cosmos to render a dbt project.
"""

from datetime import datetime

from cosmos.providers.dbt.dag import DbtDag

basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    dbt_root_path="/usr/local/airflow/dags/dbt",
    dbt_project_name="jaffle_shop",
    conn_id="airflow_db",
    dbt_args={"schema": "public"},
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dag",
)
