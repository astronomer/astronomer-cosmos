"""
An example airflow DAG that uses Cosmos to render the multi_folder dbt project with nodes grouped by folder.

Uses RenderConfig.group_nodes_by_folder to create a TaskGroup per resource type and folder
(e.g. models_a, models_b, seeds_a, seeds_b), organizing the DAG by the dbt folder structure.

Contains two DAGs:
- multi_folder_grouped_dag: uses DbtDag (dbt project as the whole DAG).
- multi_folder_grouped_task_group_dag: uses DbtTaskGroup (dbt project as a task group inside a DAG).
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import DbtDag, DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "multi_folder"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)

multi_folder_grouped_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=RenderConfig(group_nodes_by_folder=True),
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="multi_folder_grouped_dag",
    default_args={"retries": 0},
)

# Same logic as above, but using DbtTaskGroup inside a regular DAG
with DAG(
    dag_id="multi_folder_grouped_task_group_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
):
    DbtTaskGroup(
        group_id="multi_folder_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(group_nodes_by_folder=True),
        operator_args={
            "install_deps": True,
            "full_refresh": True,
        },
    )
