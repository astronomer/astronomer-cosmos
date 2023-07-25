"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)


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
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


basic_cosmos_task_group()
