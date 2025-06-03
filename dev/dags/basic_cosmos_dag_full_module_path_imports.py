"""
An example DAG that uses Cosmos by importing Cosmos classes with their full module path.
"""

import os
from datetime import datetime
from pathlib import Path

# [START cosmos_explicit_imports]
from cosmos.airflow.dag import DbtDag
from cosmos.config import ProfileConfig, ProjectConfig

# [END cosmos_explicit_imports]
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)

# [START local_example]
basic_cosmos_dag_full_module_path_imports = DbtDag(
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
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dag_full_module_path_imports",
    default_args={"retries": 0},
)
# [END local_example]
