"""
An example airflow DAG that uses Cosmos to render the multi_folder dbt project with nodes grouped by folder.

Uses RenderConfig.group_nodes_by_folder to create a TaskGroup per resource type and folder
(e.g. models_a, models_b, seeds_a, seeds_b), organizing the DAG by the dbt folder structure.

Contains three DAGs:
- multi_folder_grouped_dag: uses DbtDag (dbt project as the whole DAG).
- multi_folder_grouped_task_group_dag: uses DbtTaskGroup (dbt project as a task group inside a DAG).
- multi_folder_grouped_watcher_dag: uses DbtDag with ExecutionMode.WATCHER.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from dbt_nodes_by_folder import build_dbt_nodes_by_folder

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.config import ExecutionConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.log import get_logger
from cosmos.profiles import PostgresUserPasswordProfileMapping

logger = get_logger(__name__)

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "multi_folder_jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


def inject_folder_models_dbt_var(context, operator) -> None:
    """Inject dbt var `dbt_nodes_by_folder` (seeds, models/staging, models/production) into Cosmos tasks."""
    project_path = Path(operator.project_dir).resolve()
    dbt_nodes_by_folder = build_dbt_nodes_by_folder(project_path)
    logger.info("dbt_nodes_by_folder: %s", dbt_nodes_by_folder)
    if isinstance(operator.vars, dict):
        operator.vars["dbt_nodes_by_folder"] = dbt_nodes_by_folder
    else:
        operator.vars = {"dbt_nodes_by_folder": dbt_nodes_by_folder}


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
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH / "target/manifest.json",
        ),
        profile_config=profile_config,
        render_config=RenderConfig(group_nodes_by_folder=True, load_method=LoadMode.DBT_MANIFEST),
        operator_args={
            "install_deps": True,
            "full_refresh": True,
            "interceptors": [inject_folder_models_dbt_var],
            "trigger_rule": "all_success",
        },
    )
