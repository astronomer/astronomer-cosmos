import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
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
        WatcherKubernetesCallback,
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


@patch("cosmos.operators.watcher_kubernetes._restore_xcom_from_variable")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_skips_retry_attempt(mock_execute, mock_restore, caplog):
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
    context = {"ti": ti, "run_id": "test_run"}

    with pytest.raises(AirflowSkipException, match="does not support Airflow retries"):
        op.execute(context=context)

    mock_restore.assert_called_once_with(context)
    mock_execute.assert_not_called()


@patch("cosmos.operators.watcher_kubernetes._delete_xcom_backup_variable")
@patch("cosmos.operators.watcher_kubernetes._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_deletes_backup_on_success(mock_execute, mock_init, mock_delete):
    """Test that the XCom backup Variable is deleted after a successful execution."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    op.execute(context=context)

    mock_init.assert_called_once_with(context)
    mock_delete.assert_called_once_with(context)
    mock_execute.assert_called_once()


@patch("cosmos.operators.watcher_kubernetes._backup_xcom_to_variable")
@patch("cosmos.operators.watcher_kubernetes._delete_xcom_backup_variable")
@patch("cosmos.operators.watcher_kubernetes._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_keeps_backup_on_failure(mock_execute, mock_init, mock_delete, mock_backup):
    """Test that the XCom backup Variable is persisted (not deleted) when execution fails."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    mock_execute.side_effect = RuntimeError("dbt build failed")

    with pytest.raises(RuntimeError):
        op.execute(context=context)

    mock_init.assert_called_once_with(context)
    mock_backup.assert_called_once_with(context)
    mock_delete.assert_not_called()


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
        "`ExecutionMode.WATCHER_KUBERNETES` does not expose a DbtBuild operator, "
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
    On retry (try_number > 1) with a terminated producer, the sensor should
    fall back to executing as DbtRunKubernetesOperator by calling build_and_run_cmd.
    """
    sensor = make_sensor()
    sensor._get_producer_task_status.return_value = "success"

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--threads", "2"]
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    mock_build_and_run_cmd.assert_called_once()


@patch("cosmos.operators._watcher.base.get_xcom_val")
@patch("cosmos.operators._watcher.base.BaseConsumerSensor._log_startup_events")
def test_retry_keeps_polling_when_producer_still_running(mock_startup_events, mock_get_xcom_val):
    """
    On retry (try_number > 1) with the producer still running, the sensor
    should keep polling instead of launching a duplicate dbt run.
    """
    sensor = make_sensor()
    sensor._get_producer_task_status.return_value = "running"

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    mock_get_xcom_val.return_value = None
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is False
    assert sensor.poke_retry_number == 1


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
    assert op._tests_per_model is tests_per_model
    assert op._test_results_per_model == {}


@patch("cosmos.operators.watcher_kubernetes._delete_xcom_backup_variable")
@patch("cosmos.operators.watcher_kubernetes._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_execute_sets_context_instance_attr(mock_execute, mock_init, mock_delete):
    """execute() stores context as an instance attribute for pod_manager to use."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model={"model.pkg.orders": ["test.pkg.t1"]},
    )
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    op.execute(context=context)

    assert op._context is context


@patch("cosmos.operators.watcher_kubernetes.CosmosKubernetesPodManager")
def test_producer_pod_manager_wires_callback_extra_kwargs(mock_manager_cls):
    """pod_manager forwards tests_per_model, test_results_per_model, and context (by reference) to CosmosKubernetesPodManager."""
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model=tests_per_model,
    )
    op.client = MagicMock()
    sentinel_context = {"ti": MagicMock()}
    op._context = sentinel_context

    op.pod_manager  # noqa: B018 — access triggers cached_property creation

    mock_manager_cls.assert_called_once()
    extra = mock_manager_cls.call_args.kwargs["callback_extra_kwargs"]
    assert extra["tests_per_model"] is tests_per_model
    assert extra["test_results_per_model"] is op._test_results_per_model
    assert extra["context"] is sentinel_context


def test_pod_manager_passes_extra_kwargs_only_to_marked_callbacks():
    """callback_extra_kwargs reach WatcherKubernetesCallback but not unmarked user callbacks (#2543)."""
    from cosmos.airflow._override import CosmosKubernetesPodManager

    extra = {"tests_per_model": {"m": ["t"]}, "test_results_per_model": {}, "context": {"ti": MagicMock()}}
    manager = CosmosKubernetesPodManager(
        kube_client=MagicMock(),
        callbacks=[WatcherKubernetesCallback],
        callback_extra_kwargs=extra,
    )
    assert manager._callback_extra_kwargs is extra

    # WatcherKubernetesCallback opts in via the marker -> receives the kwargs (same object).
    assert manager._extra_kwargs_for(WatcherKubernetesCallback) is extra

    # A user-supplied callback without the marker receives nothing, so its progress_callback
    # (which may not accept Cosmos-only kwargs) is never passed them and cannot raise TypeError.
    class UserCallback:
        @staticmethod
        def progress_callback(*, line, client, mode, container_name, timestamp, pod): ...

    assert manager._extra_kwargs_for(UserCallback) == {}

    # Default when omitted is an empty dict so the spread is always safe.
    bare_manager = CosmosKubernetesPodManager(kube_client=MagicMock())
    assert bare_manager._callback_extra_kwargs == {}


@patch("cosmos.operators.watcher_kubernetes.store_dbt_resource_status_from_log")
def test_progress_callback_delegates_with_correct_args(mock_store):
    """progress_callback forwards context and test maps from kwargs."""
    mock_context = {"ti": MagicMock()}
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    test_results = {}

    WatcherKubernetesCallback.progress_callback(
        line='{"info": {"msg": "test"}}',
        client=MagicMock(),
        mode="sync",
        container_name="dbt",
        timestamp=None,
        pod=MagicMock(),
        context=mock_context,
        tests_per_model=tests_per_model,
        test_results_per_model=test_results,
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


def test_test_sensor_runs_dbt_test_on_retry():
    """On retry (try_number > 1) with a terminated producer, ``poke`` should
    invoke ``_fallback_to_non_watcher_run``, which launches a pod running
    ``dbt test --select <model>`` for this model.
    """
    sensor = make_test_sensor()
    sensor._get_producer_task_status.return_value = "success"
    sensor.build_and_run_cmd = MagicMock()
    mock_fallback = MagicMock(side_effect=sensor._fallback_to_non_watcher_run)
    sensor._fallback_to_non_watcher_run = mock_fallback

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    context = make_context(ti)

    assert sensor.poke(context) is True

    mock_fallback.assert_called_once()
    sensor.build_and_run_cmd.assert_called_once()
    _, kwargs = sensor.build_and_run_cmd.call_args
    assert kwargs["cmd_flags"] == ["--select", "stg_orders"]
    assert sensor.base_cmd == ["test"]
