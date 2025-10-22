"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode
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


operator_args = {
    "install_deps": True,  # install any necessary dependencies before running any dbt command
    "execution_timeout": timedelta(seconds=120),
}

# Currently airflow dags test ignores priority_weight and  weight_rule, for this reason, we're setting the following in the CI only:
if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"


# [START example_watcher]
example_watcher = DbtDag(
    # dbt/cosmos-specific parameters
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
    ),
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args=operator_args,
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher",
    default_args={"retries": 0},
)
# [END example_watcher]
