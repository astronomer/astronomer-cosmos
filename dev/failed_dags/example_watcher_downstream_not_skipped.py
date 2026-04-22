"""
Airflow DAG to verify that tasks downstream of a watcher DbtTaskGroup are not skipped
when the producer task is skipped on retry.

In watcher mode, when a dbt model fails the producer retries and raises AirflowSkipException.
Without Cosmos handling this, the skip propagates to all tasks downstream of the TaskGroup
(e.g. post_dbt), even though the consumer tasks inside the group succeeded.

This DAG demonstrates that:
- model_a succeeds in the producer and the consumer reads the result from XCom
- model_retry fails on the first attempt (via a fail_once sequence pre-hook) but succeeds
  on the consumer retry fallback
- post_dbt (an EmptyOperator downstream of the group) runs successfully — it is NOT skipped

A cleanup task drops the PostgreSQL sequence so the DAG can be re-triggered.
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
DBT_PROJECT_PATH = DBT_ROOT_PATH / "watcher_downstream_not_skipped"

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
        group_id="watcher_downstream_not_skipped",
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
