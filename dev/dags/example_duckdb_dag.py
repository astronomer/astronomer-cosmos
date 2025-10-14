"""
An example DAG that uses Cosmos to render a dbt-duck project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.profiles import DuckDBUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(conn_id="duckdb_default", disable_event_tracking=True),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/tmp/venv-duckdb/bin/dbt",
)

# [START local_example]
example_duckdb_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    render_config=RenderConfig(
        select=["path:seeds/raw_customers.csv", "path:models/staging/stg_customers.sql"],
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path="/tmp/venv-duckdb/bin/dbt",
    ),
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="example_duckdb_dag",
)
# [END local_example]
