"""
Demonstrate watcher-mode recovery of a downstream model that dbt skipped
because its upstream failed on the producer's first attempt (BOSS-401).

Without the fix in ``cosmos/operators/_watcher/base.py``:
- A dbt model fails on the first producer attempt.
- dbt marks every downstream node ``skipped`` with the upstream-failure cause.
- The producer log parser pushes that ``"skipped"`` status to XCom.
- The downstream consumer sensor raises ``AirflowSkipException`` -- SKIPPED.
- Airflow retries the producer task (the producer's retry is a no-op by
  design: Cosmos restores XCom and raises ``AirflowSkipException`` to avoid
  re-running the whole dbt build).
- The consumer sensor for the failing upstream retries on its own and falls
  back to running ``dbt --select <model>`` locally, which succeeds.
- The downstream consumer, however, was already SKIPPED. Airflow does not
  retry skipped tasks, so the downstream model is never re-run even though
  its upstream has now recovered.
- The DAG ends in ``success`` because Airflow treats SKIPPED as non-failure
  -- a "false green" outcome with un-materialized downstream tables.

With the fix, the producer parser rewrites the ``"skipped"`` status to
``"failed"`` for any node that dbt skipped via ``SkippingDetails`` /
``LogSkipBecauseError`` (the only paths reached when ``do_skip(cause=...)``
fires -- i.e. exclusively on upstream-node failure). The downstream consumer
then fails on attempt 1, Airflow retries it, and the same consumer-fallback
path that recovers the failing upstream now runs the downstream locally.

Models used (from ``dev/dags/dbt/watcher_upstream_failure_recovery``):
- ``model_a``: trivial source-style model, succeeds.
- ``model_flaky``: uses an ``on-run-start`` Postgres sequence to fail on
  the first ``nextval`` call (``<= 1`` → ``RAISE EXCEPTION``) and succeed
  on subsequent calls.
- ``model_downstream``: depends on ``model_flaky``; dbt skips it on
  attempt 1 because its upstream failed.

A ``post_dbt`` ``EmptyOperator`` downstream of the task group makes the
"green DAG" visible. A ``cleanup`` SQL task drops the sequence at the end
so the DAG is re-runnable from a clean state.
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
DBT_PROJECT_PATH = DBT_ROOT_PATH / "watcher_upstream_failure_recovery"

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
}

with DAG(
    dag_id="example_watcher_recovers_skipped_downstream",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
):
    dbt_group = DbtTaskGroup(
        group_id="watcher_upstream_failure_recovery",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args=operator_args,
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    cleanup = SQLExecuteQueryOperator(
        task_id="drop_fail_once_marker",
        conn_id="example_conn",
        sql="DROP SEQUENCE IF EXISTS public._cosmos_recovery_fail_once_seq;",
        trigger_rule="all_done",
    )

    dbt_group >> post_dbt
    dbt_group >> cleanup
