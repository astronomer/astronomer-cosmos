"""
An example DAG that uses Cosmos to render a dbt project as an Airflow TaskGroup.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_DIR = DBT_ROOT_PATH / "jaffle_shop"


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group_different_owners() -> None:
    """
    Example of how to override arguments / properties being run per Airflow task
    using dbt YAML.
    """

    # Considering the `dbt_project.yml` file contains the following:
    # seeds:
    #   jaffle_shop:
    #     +meta:
    #       cosmos:
    #         profile_config:
    #           profile_name: postgres_profile
    #             profile_mapping:
    #               threads: 2
    # when we run the dbt seeds commands defined in this first TaskGroup, we expect:
    # - profile_name "postgres_profile" to be used (instead of the "default")
    # - 2 threads (defined within profile_mapping) instead of the default value

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
