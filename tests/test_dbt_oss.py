import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

DBT_EXECUTABLE = shutil.which("dbt")
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev/dags/dbt/jaffle_shop"


def _is_dbt_oss() -> bool:
    if not DBT_EXECUTABLE:
        return False
    try:
        output = subprocess.run([DBT_EXECUTABLE, "--version"], capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return output.strip().startswith("dbt-oss")


@pytest.mark.integration
@pytest.mark.dbtoss
@pytest.mark.skipif(not _is_dbt_oss(), reason="Requires the dbt-oss distribution of dbt v2")
def test_dbt_oss():
    """
    Run a DbtDag using dbt-oss with duckdb and confirm it succeeds with the expected
    dbt resources and Airflow tasks, in topological order.
    """
    dbt_oss_dag = DbtDag(
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE, invocation_mode=InvocationMode.SUBPROCESS),
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="duckdb_profile",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
        ),
        render_config=RenderConfig(dbt_executable_path=DBT_EXECUTABLE, invocation_mode=InvocationMode.SUBPROCESS),
        start_date=datetime(2023, 1, 1),
        dag_id="dbt_oss_local_duckdb_dag",
        operator_args={"trigger_rule": "all_success"},
    )
    outcome = dbt_oss_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(dbt_oss_dag.dbt_graph.filtered_nodes) == 23

    tasks_names = [task.task_id for task in dbt_oss_dag.topological_sort()]
    assert tasks_names == [
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
