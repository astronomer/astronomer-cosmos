import os
from datetime import datetime
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
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

local_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_FUSION_BINARY, invocation_mode=InvocationMode.SUBPROCESS
)

watcher_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.WATCHER,
    dbt_executable_path=DBT_FUSION_BINARY,
    invocation_mode=InvocationMode.SUBPROCESS,
)


@pytest.mark.parametrize(
    "dag_id,execution_config,profile_config",
    [
        ("dbt_fusion_local_snowflake_dag", local_execution_config, snowflake_profile_config),
        ("dbt_fusion_local_bigquery_dag", local_execution_config, bigquery_profile_config),
        ("dbt_fusion_watcher_bigquery_dag", watcher_execution_config, bigquery_profile_config),
    ],
)
@pytest.mark.integration
@pytest.mark.dbtfusion
def test_dbt_fusion(dag_id, execution_config, profile_config):
    """
    Run a DbtDag using dbt Fusion.
    Confirm it succeeds and has the expected amount of both:
    - dbt resources
    - Airflow tasks
    And that the tasks are in the expected topological order.
    """
    if os.getenv("CI"):
        operator_args = {"trigger_rule": "all_success"}
    else:
        operator_args = {}

    dbt_fusion_dag = DbtDag(
        execution_config=execution_config,
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
        start_date=datetime(2023, 1, 1),
        dag_id=dag_id,
        tags=["profiles"],
        operator_args=operator_args,
    )
    outcome = dbt_fusion_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(dbt_fusion_dag.dbt_graph.filtered_nodes) == 23

    tasks_names = [task.task_id for task in dbt_fusion_dag.topological_sort()]
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
    if execution_config.execution_mode == ExecutionMode.WATCHER:
        expected_task_names.insert(0, "dbt_producer_watcher")

    assert tasks_names == expected_task_names
