import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from airflow.providers.cncf.kubernetes.secret import Secret
from packaging.version import Version

from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION

if Version(airflow_k8s_provider_version) < _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION:
    pytest.skip(
        f"Watcher Kubernetes depends on apache-airflow-providers-cncf-kubernetes >= {_K8s_WATCHER_MIN_K8S_PROVIDER_VERSION}. Currenl version: {airflow_k8s_provider_version} ",
        allow_module_level=True,
    )
else:
    from cosmos.operators.watcher_kubernetes import (
        DbtBuildWatcherKubernetesOperator,
        DbtConsumerWatcherKubernetesSensor,
        DbtProducerWatcherKubernetesOperator,
        DbtTestWatcherKubernetesOperator,
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


@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_skips_retry_attempt(mock_execute, caplog):
    """
    Test that the operator skips execution when a retry is attempted (try_number > 1).
    """
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )

    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti}

    with caplog.at_level(logging.INFO):
        result = op.execute(context=context)

    mock_execute.assert_not_called()
    assert result is None
    assert any("does not support Airflow retries" in message for message in caplog.messages)
    assert any("skipping execution" in message for message in caplog.messages)


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


@patch("cosmos.operators._watcher.base.BaseConsumerSensor._log_startup_events")
def test_first_execution_behaves_as_base_consumer_sensor(mock_startup_events):
    """
    On the first execution (try_number == 1), the sensor should poke for status
    from XCom, behaving as BaseConsumerSensor.
    """
    sensor = make_sensor()

    ti = MagicMock()
    ti.try_number = 1
    ti.xcom_pull.return_value = {"status": "success", "outlet_uris": []}
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


class _CustomCallback:
    pass


class _CustomCallback2:
    pass


@pytest.mark.parametrize(
    "callbacks_kwarg, expected_before_watcher",
    [
        pytest.param(None, [], id="none"),
        pytest.param([], [], id="empty_list"),
        pytest.param([_CustomCallback], [_CustomCallback], id="list"),
        pytest.param((_CustomCallback,), [_CustomCallback], id="tuple"),
        pytest.param(_CustomCallback, [_CustomCallback], id="single"),
        pytest.param([_CustomCallback, _CustomCallback2], [_CustomCallback, _CustomCallback2], id="multiple"),
    ],
)
def test_producer_normalizes_and_appends_watcher_callback(callbacks_kwarg, expected_before_watcher):
    """User-supplied callbacks are preserved and WatcherKubernetesCallback is appended."""
    from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

    kwargs = {"project_dir": ".", "profile_config": None, "image": "dbt-image:latest"}
    if callbacks_kwarg is not None:
        kwargs["callbacks"] = callbacks_kwarg

    op = DbtProducerWatcherKubernetesOperator(**kwargs)
    assert op.callbacks == expected_before_watcher + [WatcherKubernetesCallback]


def test_producer_stores_tests_per_model():
    """tests_per_model kwarg is stored on the operator for later use in execute()."""
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1", "test.pkg.t2"]}
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model=tests_per_model,
    )
    assert op.tests_per_model is tests_per_model
    assert op.test_results_per_model == {}


@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_execute_sets_module_globals(mock_execute):
    """execute() sets module-level globals for context and test maps."""
    import cosmos.operators.watcher_kubernetes as wk_module

    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model=tests_per_model,
    )
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    op.execute(context=context)

    assert wk_module._producer_context is context
    assert wk_module._producer_tests_per_model is tests_per_model
    assert wk_module._producer_test_results_per_model is op.test_results_per_model


@patch("cosmos.operators.watcher_kubernetes.store_dbt_resource_status_from_log")
def test_progress_callback_delegates_with_correct_args(mock_store):
    """progress_callback passes module globals to store_dbt_resource_status_from_log."""
    from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

    mock_context = {"ti": MagicMock()}
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    test_results = {}

    with (
        patch("cosmos.operators.watcher_kubernetes._producer_context", mock_context),
        patch("cosmos.operators.watcher_kubernetes._producer_tests_per_model", tests_per_model),
        patch("cosmos.operators.watcher_kubernetes._producer_test_results_per_model", test_results),
    ):
        WatcherKubernetesCallback.progress_callback(
            line='{"info": {"msg": "test"}}',
            client=MagicMock(),
            mode="sync",
            container_name="dbt",
            timestamp=None,
            pod=MagicMock(),
        )

    mock_store.assert_called_once()
    args, call_kwargs = mock_store.call_args
    assert args[0] == '{"info": {"msg": "test"}}'
    assert args[1]["context"] is mock_context
    assert call_kwargs["tests_per_model"] is tests_per_model
    assert call_kwargs["test_results_per_model"] is test_results


def make_test_sensor(**kwargs):
    extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
    kwargs["extra_context"] = extra_context
    sensor = DbtTestWatcherKubernetesOperator(
        task_id="test.stg_orders",
        project_dir="/tmp/project",
        profile_config=None,
        deferrable=False,
        image="dbt-image:latest",
        **kwargs,
    )
    sensor._get_producer_task_status = MagicMock(return_value=None)
    return sensor


def test_test_sensor_is_test_sensor_property():
    """DbtTestWatcherKubernetesOperator should report is_test_sensor=True."""
    sensor = make_test_sensor()
    assert sensor.is_test_sensor is True


@pytest.mark.parametrize(
    "xcom_return, expected",
    [
        pytest.param("pass", True, id="pass"),
        pytest.param("fail", AirflowException, id="fail"),
        pytest.param(None, False, id="waiting"),
    ],
)
def test_test_sensor_poke_status(xcom_return, expected):
    """Test that the test sensor correctly handles each aggregated test status."""
    from cosmos.operators._watcher.aggregation import get_tests_status_xcom_key

    sensor = make_test_sensor()
    model_uid = "model.jaffle_shop.stg_orders"
    tests_xcom_key = get_tests_status_xcom_key(model_uid)

    ti = MagicMock()
    ti.try_number = 1

    def xcom_side_effect(task_ids=None, key=None):
        if key == tests_xcom_key:
            return xcom_return
        return None

    ti.xcom_pull.side_effect = xcom_side_effect
    context = make_context(ti)

    if expected is AirflowException:
        with pytest.raises(AirflowException):
            sensor.poke(context)
    else:
        assert sensor.poke(context) is expected

    ti.xcom_pull.assert_any_call(sensor.producer_task_id, key=tests_xcom_key)


def test_test_sensor_raises_on_retry():
    """On retry (try_number > 1), poke should raise because test re-execution is not supported."""
    sensor = make_test_sensor()

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    context = make_context(ti)

    with pytest.raises(AirflowException, match="not yet supported"):
        sensor.poke(context)
