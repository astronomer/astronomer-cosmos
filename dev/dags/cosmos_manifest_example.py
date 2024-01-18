"""
An example DAG that uses Cosmos to render a dbt project.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, LoadMode, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping, DbtProfileConfigVars

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
        dbt_config_vars=DbtProfileConfigVars(send_anonymous_usage_stats=True),
    ),
)

# [START local_example]
cosmos_manifest_example = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        manifest_path=DBT_ROOT_PATH / "jaffle_shop" / "target" / "manifest.json",
        project_name="jaffle_shop",
    ),
    profile_config=profile_config,
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, select=["path:seeds/raw_customers.csv"]),
    execution_config=ExecutionConfig(dbt_project_path=DBT_ROOT_PATH / "jaffle_shop"),
    operator_args={"install_deps": True},
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="cosmos_manifest_example",
    default_args={"retries": 2},
)
# [END local_example]
