"""
Run Cosmos against dbt-core 2.0 (the Rust engine) through ``InvocationMode.SUBPROCESS`` on duckdb.

dbt-core 2.0 ships a Python ``dbt`` package without the 1.x API Cosmos drives in-process (no ``dbt.version``,
no ``dbtRunner`` callbacks, ``ls`` results without JSON records), so both rendering and execution go through
the ``dbt`` binary. See https://github.com/astronomer/astronomer-cosmos/issues/2993
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev/dags/dbt/jaffle_shop"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)


def dbt_core_2_binary() -> str:
    dbt_executable = shutil.which("dbt")
    assert dbt_executable, "dbt-core 2.0 must be installed in the test environment"
    return dbt_executable


def duckdb_profile_config(tmp_path: Path) -> ProfileConfig:
    # Every task runs dbt in its own copy of the project, so the duckdb file must live outside it.
    profiles_yml = tmp_path / "profiles.yml"
    profiles_yml.write_text(f"""duckdb_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {tmp_path / "jaffle_shop.duckdb"}
      threads: 4
""")
    return ProfileConfig(profile_name="duckdb_profile", target_name="dev", profiles_yml_filepath=profiles_yml)


@pytest.mark.parametrize(
    "dag_id,execution_mode",
    [
        ("dbt_core_2_local_duckdb_dag", ExecutionMode.LOCAL),
        ("dbt_core_2_watcher_duckdb_dag", ExecutionMode.WATCHER),
    ],
)
@pytest.mark.integration
@pytest.mark.dbtcore2
def test_dbt_core_2(dag_id, execution_mode, tmp_path):
    """
    Run a DbtDag using dbt-core 2.0 with rendering and execution both in InvocationMode.SUBPROCESS.
    Confirm it succeeds and has the expected amount of both:
    - dbt resources
    - Airflow tasks
    And that the tasks are in the expected topological order.
    """
    dbt_executable_path = dbt_core_2_binary()

    if os.getenv("CI"):
        operator_args = {"trigger_rule": "all_success"}
    else:
        operator_args = {}

    dbt_core_2_dag = DbtDag(
        execution_config=ExecutionConfig(
            execution_mode=execution_mode,
            dbt_executable_path=dbt_executable_path,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        project_config=project_config,
        profile_config=duckdb_profile_config(tmp_path),
        render_config=RenderConfig(
            dbt_executable_path=dbt_executable_path,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        start_date=datetime(2023, 1, 1),
        dag_id=dag_id,
        tags=["profiles"],
        operator_args=operator_args,
    )
    outcome = dbt_core_2_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(dbt_core_2_dag.dbt_graph.filtered_nodes) == 23

    tasks_names = [task.task_id for task in dbt_core_2_dag.topological_sort()]
    expected_task_names = [
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers.run",
        "stg_customers.test",
        "stg_orders.run",
        "stg_orders.test",
        "stg_payments.run",
        "stg_payments.test",
        "customers.run",
        "customers.test",
        "orders.run",
        "orders.test",
    ]
    if execution_mode == ExecutionMode.WATCHER:
        expected_task_names.insert(0, "dbt_producer_watcher")

    assert tasks_names == expected_task_names
