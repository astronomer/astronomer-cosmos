"""
An example DAG that uses Cosmos to render a dbt project as an Airflow TaskGroup.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_DIR = DBT_ROOT_PATH / "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default", target_name="dev", profiles_yml_filepath=PROJECT_DIR / "profiles.yml"
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group_different_owners() -> None:
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """

    non_models = DbtTaskGroup(
        group_id="seeds",
        project_config=ProjectConfig(PROJECT_DIR.as_posix()),
        render_config=RenderConfig(
            exclude=["path:models"],
        ),
        operator_args={"install_deps": True},
        profile_config=profile_config,
    )

    models = DbtTaskGroup(
        group_id="models",
        project_config=ProjectConfig(PROJECT_DIR.as_posix()),
        render_config=RenderConfig(
            select=["path:models"],
        ),
        operator_args={"install_deps": True},
        profile_config=profile_config,
    )

    non_models >> models


basic_cosmos_task_group_different_owners()
