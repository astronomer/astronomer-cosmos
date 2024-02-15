"""
A DAG that uses Cosmos to render a dbt project for performance testing.
"""

import airflow
from datetime import datetime
import os
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_SQLITE_PATH = str(DEFAULT_DBT_ROOT_PATH / "data")

profile_config = ProfileConfig(
    profile_name="simple",
    target_name="dev",
    profiles_yml_filepath=(DBT_ROOT_PATH / "simple/profiles.yml"),
)

cosmos_perf_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "perf",
        env_vars={"DBT_SQLITE_PATH": DBT_SQLITE_PATH},
    ),
    profile_config=profile_config,
    render_config=RenderConfig(
        dbt_deps=False,
    ),
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="performance_dag",
)
