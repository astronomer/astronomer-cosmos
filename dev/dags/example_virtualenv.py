"""
An example DAG that uses Cosmos to render a dbt project as an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.configuration import get_airflow_home
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


# [START virtualenv_example]
@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def example_virtualenv() -> None:
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = EmptyOperator(task_id="end-venv-examples")

    tmp_venv_task_group = DbtTaskGroup(
        group_id="tmp-venv-group",
        # dbt/cosmos-specific parameters
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            # We can enable this flag if we want Airflow to create one virtualenv
            # and reuse that within the whole DAG.
            # virtualenv_dir=f"{get_airflow_home()}/persistent-venv",
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-postgres==1.6.0b1"],
            "install_deps": True,
            "emit_datasets": False,  # Example of how to not set inlets and outlets
        },
    )

    cached_venv_task_group = DbtTaskGroup(
        group_id="cached-venv-group",
        # dbt/cosmos-specific parameters
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            # We can enable this flag if we want Airflow to create one virtualenv
            # and reuse that within the whole DAG.
            virtualenv_dir=Path(f"{get_airflow_home()}/persistent-venv"),
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-postgres==1.6.0b1"],
            "install_deps": True,
        },
    )

    start_task >> [tmp_venv_task_group, cached_venv_task_group] >> end_task


example_virtualenv()
# [END virtualenv_example]
