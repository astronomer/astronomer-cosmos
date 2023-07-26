"""
A DAG that uses Cosmos with a custom profile.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROFILES_FILE_PATH = Path(DBT_ROOT_PATH, "jaffle_shop", "profiles.yml")


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def user_defined_profile() -> None:
    """
    A DAG that uses Cosmos with a custom profile.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profiles_yml_filepath=PROFILES_FILE_PATH,
        ),
        operator_args={
            "append_env": True,
        },
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


user_defined_profile()
