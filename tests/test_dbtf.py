from datetime import datetime
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

DBT_FUSION_BINARY = Path.home() / ".local/bin/dbt"
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

profile_config = ProfileConfig(
    profile_name="snowflake_profile",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_YAML_FILEPATH,
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_FUSION_BINARY)


@pytest.mark.integration
@pytest.mark.dbtFusion
def test_dbt_dag_with_dbt_fusion():
    """
    Run a DbtDag using dbt Fusion.
    Confirm it succeeds and has the expected amount of both:
    - dbt resources
    - Airflow tasks
    And that the tasks are in the expected topological order.
    """
    snowflake_dag = DbtDag(
        execution_config=execution_config,
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="snowflake_dbt_fusion_dag",
        tags=["profiles"],
    )
    outcome = snowflake_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(snowflake_dag.dbt_graph.filtered_nodes) == 26

    assert len(snowflake_dag.task_dict) == 13
    tasks_names = [task.task_id for task in snowflake_dag.topological_sort()]
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
    assert tasks_names == expected_task_names
