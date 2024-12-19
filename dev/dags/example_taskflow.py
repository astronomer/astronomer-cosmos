import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


@task(task_id="build_partial_dbt_env_vars_operator")
def build_partial_dbt_env():
    return {"ENV_VAR_NAME": "value", "ENV_VAR_NAME_2": False}


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def example_taskflow() -> None:
    DbtTaskGroup(
        group_id="transform_task_group",
        project_config=ProjectConfig(
            dbt_project_path=DBT_ROOT_PATH / "jaffle_shop",
            manifest_path=DBT_ROOT_PATH / "jaffle_shop" / "target" / "manifest.json",
            env_vars=build_partial_dbt_env(),
        ),
        profile_config=profile_config,
        operator_args={"install_deps": True},
    )


example_taskflow()
