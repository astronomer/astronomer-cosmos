"""Reproduce issue #2625: concurrent watcher producers share one xcom-backup key.

Two parallel DbtTaskGroups using ExecutionMode.WATCHER race on the same Airflow
Variable key (``cosmos_xcom_backup__{dag_id}__{run_id}``) because
``_get_task_group_id()`` in ``cosmos/operators/_watcher/xcom.py`` reads
``task.task_group_id`` -- an attribute that does not exist on Airflow operators.
The helper always returns None, so the task-group discriminator is dropped from
the key and every producer task in the same DAG run computes the same key. On
first model completion in either producer, the loser of the INSERT race logs a
``UniqueViolation``.

Reproduction conditions (from the issue):
- both groups start in the same DAG run (no dependency between them -> parallel)
- each group must have at least one model complete while both producers run
- Airflow must be running with multiple workers (not LocalExecutor with
  parallelism=1)
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dags/dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "jaffle_shop"

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
    invocation_mode=InvocationMode.DBT_RUNNER,
)

operator_args = {
    "install_deps": True,
    "execution_timeout": timedelta(seconds=120),
}

if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"


with DAG(
    dag_id="example_watcher_xcom_collision",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 0},
):
    group_a = DbtTaskGroup(
        group_id="group_a",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(select=["+customers"]),
        operator_args=operator_args,
    )

    group_b = DbtTaskGroup(
        group_id="group_b",
        execution_config=execution_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(select=["+orders"]),
        operator_args=operator_args,
    )
    # No dependency between group_a and group_b -- both run in parallel.
