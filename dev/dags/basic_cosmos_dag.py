"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

# [START cosmos_init_imports]
from cosmos import DbtDag, ProfileConfig, ProjectConfig

# [END cosmos_init_imports]
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME


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
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dag",
    default_args={"retries": 0},
)
# [END local_example]
