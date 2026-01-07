import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.secret import Secret

from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from cosmos.operators.watcher_kubernetes import (
    DbtBuildWatcherKubernetesOperator,
    DbtConsumerWatcherKubernetesSensor,
    DbtProducerWatcherKubernetesOperator,
)

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


def test_retries_set_to_zero_on_init():
    """
    Test that the operator sets retries to 0 during initialization.
    """
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    assert op.retries == 0


def test_retries_overridden_even_if_user_sets_them():
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
def test_blocks_retry_attempt(mock_execute, caplog):
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


def test_raises_exception_when_task_instance_missing():
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


def test_dbt_build_watcher_kubernetes_operator_raises_not_implemented_error():
    expected_message = (
        "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
        "since the build command is executed by the producer task."
    )

    with pytest.raises(NotImplementedError, match=expected_message):
        DbtBuildWatcherKubernetesOperator()


def make_sensor(**kwargs):
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


def make_context(ti_mock, *, run_id: str = "test-run", map_index: int = 0):
    return {
        "ti": ti_mock,
        "run_id": run_id,
        "task_instance": MagicMock(map_index=map_index),
    }


def test_first_execution_behaves_as_base_consumer_sensor():
    """
    On the first execution (try_number == 1), the sensor should poke for status
    from XCom, behaving as BaseConsumerSensor.
    """
    sensor = make_sensor()

    ti = MagicMock()
    ti.try_number = 1
    ti.xcom_pull.return_value = "success"
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    ti.xcom_pull.assert_called()


@patch("cosmos.operators.kubernetes.DbtKubernetesBaseOperator.build_and_run_cmd")
def test_retry_executes_as_dbt_run_kubernetes_operator(mock_build_and_run_cmd):
    """
    On retry (try_number > 1), the sensor should fall back to executing
    as DbtRunKubernetesOperator by calling build_and_run_cmd.
    """
    sensor = make_sensor()

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--threads", "2"]
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    mock_build_and_run_cmd.assert_called_once()


def test_use_event_returns_false():
    """
    DbtConsumerWatcherKubernetesSensor should return False for use_event(),
    meaning it uses XCom-based status retrieval instead of events.
    """
    sensor = make_sensor()
    assert sensor.use_event() is False
