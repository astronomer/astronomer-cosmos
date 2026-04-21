"""
Airflow DAG to demonstrate watcher retry behaviour where a model fails on first attempt but succeeds on retry.

model_a always succeeds. model_retry uses the fail_once macro which creates a marker table
and fails on the first invocation; on retry the marker exists and the model succeeds.

A cleanup task at the end drops the marker table so the DAG can be re-triggered.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dags/dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "watcher_retry_succeeds"

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
)

operator_args = {
    "install_deps": True,
    "execution_timeout": timedelta(seconds=120),
}

if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
    "depends_on_past": True,
}

with DAG(
    dag_id="example_watcher_retry_succeeds",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
):
    dbt_group = DbtTaskGroup(
        group_id="watcher_retry_succeeds",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args=operator_args,
    )

    cleanup = SQLExecuteQueryOperator(
        task_id="drop_fail_once_marker",
        conn_id="example_conn",
        sql="DROP SEQUENCE IF EXISTS public._cosmos_fail_once_seq;",
        trigger_rule="all_done",
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    dbt_group >> post_dbt
    dbt_group >> cleanup
