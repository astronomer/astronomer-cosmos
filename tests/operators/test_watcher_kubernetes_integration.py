import os
from datetime import datetime
from pathlib import Path

import pytest
from airflow.providers.cncf.kubernetes.secret import Secret

from cosmos import DbtDag
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import AIRFLOW_VERSION, ExecutionMode, LoadMode, TestBehavior, Version
from cosmos.operators.watcher_kubernetes import (
    DbtProducerWatcherKubernetesOperator,
    DbtRunWatcherKubernetesOperator,
    DbtSeedWatcherKubernetesOperator,
)
from tests.utils import run_dag

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt"

DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
AIRFLOW_DBT_PROJECT_DIR = DBT_ROOT_PATH / "jaffle_shop"

K8S_PROJECT_DIR = "dags/dbt/jaffle_shop"
KBS_DBT_PROFILES_YAML_FILEPATH = Path(K8S_PROJECT_DIR) / "profiles.yml"

DBT_IMAGE = "dbt-jaffle-shop:1.0.0"

project_seeds = [{"project": "jaffle_shop", "seeds": ["raw_customers", "raw_payments", "raw_orders"]}]

postgres_password_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_PASSWORD",
    secret="postgres-secrets",
    key="password",
)

postgres_host_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_HOST",
    secret="postgres-secrets",
    key="host",
)

operator_args = {
    "deferrable": False,
    "image": DBT_IMAGE,
    "get_logs": True,
    "is_delete_operator_pod": False,
    "log_events_on_failure": True,
    "secrets": [postgres_password_secret, postgres_host_secret],
    "env_vars": {
        "POSTGRES_DB": "postgres",
        "POSTGRES_SCHEMA": "public",
        "POSTGRES_USER": "postgres",
    },
    "retry": 0,
}

profile_config = ProfileConfig(
    profile_name="postgres_profile", target_name="dev", profiles_yml_filepath=KBS_DBT_PROFILES_YAML_FILEPATH
)

project_config = ProjectConfig(
    project_name="jaffle_shop",
    manifest_path=AIRFLOW_DBT_PROJECT_DIR / "target/manifest.json",
)

render_config = RenderConfig(load_method=LoadMode.DBT_MANIFEST, test_behavior=TestBehavior.NONE)


# Currently airflow dags test ignores priority_weight and weight_rule, for this reason, we're setting the following in the CI only:
if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"


@pytest.mark.skipif(
    AIRFLOW_VERSION < Version("3.1"),
    reason="We are only testing watcher Kubernetes with Airflow 3.1 and more recent versions of the K8s provider",
)
@pytest.mark.integration
def test_dbt_dag_with_watcher_kubernetes():
    """
    Create an Cosmos DbtDag with `ExecutionMode.WATCHER_KUBERNETES`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """

    dag_dbt_watcher_kubernetes = DbtDag(
        dag_id="watcher_kubernetes_dag",
        start_date=datetime(2022, 11, 27),
        doc_md=__doc__,
        catchup=False,
        # Cosmos-specific parameters:
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER_KUBERNETES,
            dbt_project_path=K8S_PROJECT_DIR,
        ),
        operator_args=operator_args,
    )

    run_dag(dag_dbt_watcher_kubernetes)

    assert len(dag_dbt_watcher_kubernetes.task_dict) == 9
    tasks_names = [task.task_id for task in dag_dbt_watcher_kubernetes.topological_sort()]

    expected_task_names = [
        "dbt_producer_watcher",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers_run",
        "stg_orders_run",
        "stg_payments_run",
        "customers_run",
        "orders_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(
        dag_dbt_watcher_kubernetes.task_dict["dbt_producer_watcher"],
        DbtProducerWatcherKubernetesOperator,
    )
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["raw_customers_seed"], DbtSeedWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["raw_orders_seed"], DbtSeedWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["raw_payments_seed"], DbtSeedWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["stg_customers_run"], DbtRunWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["stg_orders_run"], DbtRunWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["stg_payments_run"], DbtRunWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["customers_run"], DbtRunWatcherKubernetesOperator)
    assert isinstance(dag_dbt_watcher_kubernetes.task_dict["orders_run"], DbtRunWatcherKubernetesOperator)

    expected_downstream_task_ids = {
        "raw_payments_seed",
        "raw_orders_seed",
        "raw_customers_seed",
    }
    assert (
        dag_dbt_watcher_kubernetes.task_dict["dbt_producer_watcher"].downstream_task_ids == expected_downstream_task_ids
    )
