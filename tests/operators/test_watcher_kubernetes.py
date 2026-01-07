import logging
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.secret import Secret

from cosmos import DbtDag
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import AIRFLOW_VERSION, ExecutionMode, LoadMode, TestBehavior, Version
from cosmos.operators.watcher_kubernetes import (
    DbtBuildWatcherKubernetesOperator,
    DbtConsumerWatcherKubernetesSensor,
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


class TestDbtProducerWatcherKubernetesOperator:
    """
    Tests for DbtProducerWatcherKubernetesOperator.

    The producer operator does not support Airflow retries because re-running
    a dbt build would cause issues with the watcher pattern.
    """

    def test_retries_set_to_zero_on_init(self):
        """
        Test that the operator sets retries to 0 during initialization.
        """
        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
        )
        assert op.retries == 0

    def test_retries_overridden_even_if_user_sets_them(self):
        """
        Test that even if a user explicitly sets retries, they are overridden to 0.
        """
        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            retries=5,
        )
        assert op.retries == 0

    @patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
    def test_blocks_retry_attempt(self, mock_execute, caplog):
        """
        Test that the operator raises an AirflowException when a retry is attempted (try_number > 1).
        """
        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
        )

        ti = MagicMock()
        ti.try_number = 2
        context = {"ti": ti}

        with caplog.at_level(logging.ERROR):
            with pytest.raises(AirflowException) as excinfo:
                op.execute(context=context)

        mock_execute.assert_not_called()
        assert "does not support Airflow retries" in str(excinfo.value)
        assert any("does not support Airflow retries" in message for message in caplog.messages)

    def test_raises_exception_when_task_instance_missing(self):
        """
        Test that the operator raises an AirflowException when task instance is missing from context.
        """
        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
        )

        context = {"ti": None}

        with pytest.raises(AirflowException) as excinfo:
            op.execute(context=context)

        assert "expects a task instance" in str(excinfo.value)


class TestDbtBuildWatcherKubernetesOperator:

    def test_dbt_build_watcher_kubernetes_operator_raises_not_implemented_error(self):
        expected_message = (
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )

        with pytest.raises(NotImplementedError, match=expected_message):
            DbtBuildWatcherKubernetesOperator()


class TestDbtConsumerWatcherKubernetesSensor:
    """
    Tests for DbtConsumerWatcherKubernetesSensor.

    On first execution, it behaves as BaseConsumerSensor (pokes for status from XCom).
    On retry (try_number > 1), it falls back to executing as DbtRunKubernetesOperator.
    """

    def make_sensor(self, **kwargs):
        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        kwargs["extra_context"] = extra_context
        sensor = DbtConsumerWatcherKubernetesSensor(
            task_id="model.my_model",
            project_dir="/tmp/project",
            profile_config=None,
            deferrable=False,
            image="dbt-image:latest",
            **kwargs,
        )
        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    def make_context(self, ti_mock, *, run_id: str = "test-run", map_index: int = 0):
        return {
            "ti": ti_mock,
            "run_id": run_id,
            "task_instance": MagicMock(map_index=map_index),
        }

    def test_first_execution_behaves_as_base_consumer_sensor(self):
        """
        On the first execution (try_number == 1), the sensor should poke for status
        from XCom, behaving as BaseConsumerSensor.
        """
        sensor = self.make_sensor()

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = "success"
        context = self.make_context(ti)

        result = sensor.poke(context)

        assert result is True
        ti.xcom_pull.assert_called()

    @patch("cosmos.operators.kubernetes.DbtKubernetesBaseOperator.build_and_run_cmd")
    def test_retry_executes_as_dbt_run_kubernetes_operator(self, mock_build_and_run_cmd):
        """
        On retry (try_number > 1), the sensor should fall back to executing
        as DbtRunKubernetesOperator by calling build_and_run_cmd.
        """
        sensor = self.make_sensor()

        ti = MagicMock()
        ti.try_number = 2
        ti.xcom_pull.return_value = None
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--threads", "2"]
        context = self.make_context(ti)

        result = sensor.poke(context)

        assert result is True
        mock_build_and_run_cmd.assert_called_once()

    def testuse_event_returns_false(self):
        """
        DbtConsumerWatcherKubernetesSensor should return False for use_event(),
        meaning it uses XCom-based status retrieval instead of events.
        """
        sensor = self.make_sensor()
        assert sensor.use_event() is False
