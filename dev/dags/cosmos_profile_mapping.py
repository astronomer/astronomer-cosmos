"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.

It uses the automatic profile rendering from an Airflow connection.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import get_automatic_profile_mapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def cosmos_profile_mapping() -> None:
    """
    Turns a dbt project into a TaskGroup with a profile mapping.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profile_mapping=get_automatic_profile_mapping(
                conn_id="airflow_db",
                profile_args={"schema": "public"},
            ),
        ),
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


cosmos_profile_mapping()
