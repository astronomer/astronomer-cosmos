"""
An example DAG that uses Cosmos to render a dbt-duck project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_default", profile_args={"path": "jaffle_shop.duck_db"}, disable_event_tracking=True
    ),
)

# [START local_example]
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dbt_duckdb",
)
# [END local_example]
