"""
Variant of ``example_watcher_downstream_not_skipped`` that opts into JSON-formatted dbt
logs on the watcher consumer's retry, via
``operator_args={"dbt_cmd_flags": ["--log-format", "json"]}``.

Use this DAG to verify that:

1. The producer's hardcoded ``--log-format json`` (used internally for event-stream parsing)
   is stripped from the consumer's retry dbt command — i.e. the fix in
   ``cosmos/operators/_watcher/base.py`` (``_filter_flags``) is working.
2. When the user *explicitly* opts in via ``dbt_cmd_flags``, the consumer's retry dbt
   command does emit JSON-formatted output (since ``dbt_cmd_flags`` is appended by
   ``build_cmd`` outside the filter pipeline).

Expected behaviour on the retry of ``watcher_downstream_not_skipped_json_logs.model_retry_run``:

- Log lines are structured JSON objects (``{"data": ..., "info": ...}``) — exactly what
  the user asked for. Without the ``dbt_cmd_flags`` opt-in, the same retry would emit
  dbt's default text format (see ``example_watcher_downstream_not_skipped``).

Reuses the same dbt project as the original DAG (``watcher_downstream_not_skipped``) and
the same ``public._cosmos_fail_once_seq`` sequence — do **not** run both DAGs concurrently
or they will race on the fail-once marker.
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

# The user opt-in for JSON-formatted dbt output on the consumer's retry.
operator_args = {
    "install_deps": True,
    "execution_timeout": timedelta(seconds=120),
    "dbt_cmd_flags": ["--log-format", "json"],
}

if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
    "depends_on_past": True,
}

with DAG(
    dag_id="example_watcher_downstream_not_skipped_json_logs",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
):
    dbt_group = DbtTaskGroup(
        group_id="watcher_downstream_not_skipped_json_logs",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args=operator_args,
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    cleanup = SQLExecuteQueryOperator(
        task_id="drop_fail_once_marker",
        conn_id="example_conn",
        sql="DROP SEQUENCE IF EXISTS public._cosmos_fail_once_seq;",
        trigger_rule="all_done",
    )

    dbt_group >> post_dbt
    dbt_group >> cleanup
