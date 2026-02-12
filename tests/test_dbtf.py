from datetime import datetime
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DBT_FUSION_BINARY = Path.home() / ".local/bin/dbt"
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

snowflake_profile_config = ProfileConfig(
    profile_name="snowflake_profile",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_YAML_FILEPATH,
)

bigquery_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp_gs_conn",
        profile_args={"dataset": "release_17", "project": "astronomer-dag-authoring"},
        disable_event_tracking=True,
    ),
)

render_config = RenderConfig(dbt_executable_path=DBT_FUSION_BINARY, invocation_mode=InvocationMode.SUBPROCESS)

execution_config = ExecutionConfig(dbt_executable_path=DBT_FUSION_BINARY, invocation_mode=InvocationMode.SUBPROCESS)


@pytest.mark.integration
@pytest.mark.dbtfusion
def test_dbt_snowflake_dag_with_dbt_fusion():
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
        profile_config=snowflake_profile_config,
        render_config=render_config,
        start_date=datetime(2023, 1, 1),
        dag_id="snowflake_dbt_fusion_dag",
        tags=["profiles"],
    )
    outcome = snowflake_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(snowflake_dag.dbt_graph.filtered_nodes) == 23

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


@pytest.mark.integration
@pytest.mark.dbtfusion
def test_dbt_bigquery_dag_with_dbt_fusion():
    """
    Run a DbtDag using dbt Fusion.
    Confirm it succeeds and has the expected amount of both:
    - dbt resources
    - Airflow tasks
    And that the tasks are in the expected topological order.
    """
    bigquery_dag = DbtDag(
        execution_config=execution_config,
        project_config=project_config,
        profile_config=bigquery_profile_config,
        render_config=render_config,
        start_date=datetime(2023, 1, 1),
        dag_id="bigquery_dbt_fusion_dag",
        tags=["profiles"],
    )
    outcome = bigquery_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(bigquery_dag.dbt_graph.filtered_nodes) == 23

    assert len(bigquery_dag.task_dict) == 13
    tasks_names = [task.task_id for task in bigquery_dag.topological_sort()]
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
