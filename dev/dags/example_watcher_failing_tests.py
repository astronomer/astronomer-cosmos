"""
DAG to reproduce the watcher bug reported in https://github.com/astronomer/astronomer-cosmos/issues/2554

Two models: model_a (succeeds) and model_f (fails - references nonexistent column from model_a).
Producer retries with retries=2. To reproduce, limit parallelism:
    export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_NAME = "watcher_failing_tests"
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


example_watcher_failing_tests = DbtDag(
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "execution_timeout": timedelta(seconds=120),
    },
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher_failing_tests",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=0),
    },
)
