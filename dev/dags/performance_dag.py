"""
An Airflow DAG that uses Cosmos to render a dbt project for performance testing.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)

cosmos_perf_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "perf",
    ),
    profile_config=profile_config,
    render_config=RenderConfig(
        dbt_deps=False,
    ),
    # normal dag parameters
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="performance_dag",
)
