"""
Example DAG that uses ExecutionMode.WATCHER with dbt_retry_count to demonstrate
producer-internal dbt retry for transient failures.

The jaffle_shop project includes a flaky_model that fails on its first attempt
(division by zero) and succeeds on dbt retry (pre-hook increments a flag).
The flaky_model_cleanup model depends on flaky_model and resets the flag so
each DAG run starts fresh.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
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
    "install_deps": True,
    "execution_timeout": timedelta(seconds=120),
}

# [START example_watcher_retry]
example_watcher_retry = DbtDag(
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        setup_operator_args={
            "dbt_retry_count": 2,
        },
    ),
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=RenderConfig(exclude=["raw_payments"]),
    operator_args=operator_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher_retry",
    default_args={"retries": 0},
)
# [END example_watcher_retry]
