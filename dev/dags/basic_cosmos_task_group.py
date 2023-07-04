"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group() -> None:
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        dbt_root_path=DBT_ROOT_PATH,
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


basic_cosmos_task_group()
