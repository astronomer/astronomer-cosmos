from datetime import datetime, timedelta
from pathlib import Path

import pytest
from airflow import DAG
from airflow.utils.state import DagRunState

from cosmos import DbtTaskGroup
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import AIRFLOW_VERSION, ExecutionMode, TestBehavior, Version
from cosmos.operators.watcher_kubernetes import (
    DbtProducerKubernetesWatcherOperator,
    DbtRunKubernetesWatcherOperator,
    DbtSeedKubernetesWatcherOperator,
)

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"


project_config = ProjectConfig(
    project_name="jaffle_shop",
    manifest_path=DBT_PROJECT_PATH / "target/manifest.json",
)

profile_config = ProfileConfig(
    profile_name="default", target_name="dev", profiles_yml_filepath=DBT_PROFILES_YAML_FILEPATH
)


@pytest.mark.skipif(
    AIRFLOW_VERSION < Version("3.1"),
    reason="We are only testing watcher Kubernetes with Airflow 3.1 and more recent versions of the K8s provider",
)
@pytest.mark.integration
def test_dbt_task_group_with_watcher_kubernetes():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """

    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
    }

    with DAG(
        dag_id="example_watcher_taskgroup",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher:
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        pre_dbt = EmptyOperator(task_id="pre_dbt")

        dbt_task_group = DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

        pre_dbt
        dbt_task_group

    outcome = dag_dbt_task_group_watcher.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(dag_dbt_task_group_watcher.task_dict) == 10
    tasks_names = [task.task_id for task in dag_dbt_task_group_watcher.topological_sort()]

    expected_task_names = [
        "pre_dbt",
        "dbt_task_group.dbt_producer_watcher",
        "dbt_task_group.raw_customers_seed",
        "dbt_task_group.raw_orders_seed",
        "dbt_task_group.raw_payments_seed",
        "dbt_task_group.stg_customers_run",
        "dbt_task_group.stg_orders_run",
        "dbt_task_group.stg_payments_run",
        "dbt_task_group.customers_run",
        "dbt_task_group.orders_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"],
        DbtProducerKubernetesWatcherOperator,
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_customers_seed"], DbtSeedKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_orders_seed"], DbtSeedKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_payments_seed"], DbtSeedKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_customers_run"], DbtRunKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_orders_run"], DbtRunKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_payments_run"], DbtRunKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.customers_run"], DbtRunKubernetesWatcherOperator
    )
    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.orders_run"], DbtRunKubernetesWatcherOperator
    )

    assert dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"].downstream_task_ids == set()
