"""
DAG to reproduce the watcher bug reported in https://github.com/astronomer/astronomer-cosmos/issues/2554

Two models: model_a (succeeds) and model_f (fails - references nonexistent column from model_a).
Producer retries with retries=2. To reproduce, limit parallelism:
    export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import DAG

from cosmos import DbtDag, DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
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

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.WATCHER,
    invocation_mode=InvocationMode.SUBPROCESS,
)

operator_args = {
    "install_deps": True,
    "execution_timeout": timedelta(seconds=120),
}

# Currently airflow dags test ignores priority_weight and weight_rule, for this reason, we're setting the following in the CI only:
if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}


example_watcher_failing_tests = DbtDag(
    execution_config=execution_config,
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args=operator_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher_failing_tests",
    default_args=default_args,
)


with DAG(
    dag_id="example_watcher_failing_tests_taskgroup",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
):
    DbtTaskGroup(
        group_id="watcher_failing_tests",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args=operator_args,
    )
