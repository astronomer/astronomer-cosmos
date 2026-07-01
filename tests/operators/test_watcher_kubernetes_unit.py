import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from airflow.providers.cncf.kubernetes.secret import Secret
from packaging.version import Version

from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    PRODUCER_WATCHER_TASK_ID,
    LoadMode,
    TestBehavior,
    _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION,
)

if Version(airflow_k8s_provider_version) < _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION:
    pytest.skip(
        f"Watcher Kubernetes depends on apache-airflow-providers-cncf-kubernetes >= {_K8s_WATCHER_MIN_K8S_PROVIDER_VERSION}. Currenl version: {airflow_k8s_provider_version} ",
        allow_module_level=True,
    )
else:
    from cosmos.operators._k8s_common import CONTEXT_HOLDER_KEY, CONTEXT_KEY
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


def test_producer_default_task_id_matches_watcher_task_id():
    """The Kubernetes producer must default its ``task_id`` to ``PRODUCER_WATCHER_TASK_ID``.

    Consumer sensors default ``producer_task_id`` to ``PRODUCER_WATCHER_TASK_ID``, so a
    directly-instantiated producer with a different default would make consumers poll the
    wrong task and hang. The Cosmos-rendered graph always sets ``task_id`` explicitly, but
    this guards direct instantiation and keeps parity with ``DbtProducerWatcherOperator``.
    """
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    assert op.task_id == PRODUCER_WATCHER_TASK_ID


def test_producer_honours_explicit_task_id():
    """An explicitly-provided ``task_id`` is still respected."""
    op = DbtProducerWatcherKubernetesOperator(
        task_id="custom_producer",
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    assert op.task_id == "custom_producer"


@patch("cosmos.operators._k8s_common._restore_xcom_from_variable")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_skips_retry_attempt(mock_execute, mock_restore):
    """Smoke-test that the K8s producer delegates to ``execute_watcher_producer``.

    Full coverage of the producer's retry / XCom backup behaviour lives in
    ``test_k8s_common.py``; this just guards against a missing delegation.
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


def test_producer_uses_watcher_k8s_callback():
    """Test that the WatcherK8sCallback is included in the producer's callbacks."""
    from cosmos.operators._k8s_common import WatcherK8sCallback

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    assert WatcherK8sCallback in op.callbacks


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


@patch("cosmos.operators._k8s_common.CosmosKubernetesPodManager")
def test_producer_pod_manager_wires_callback_extra_kwargs(mock_manager_cls):
    """pod_manager forwards tests_per_model, test_results_per_model, and the context holder (by reference) to CosmosKubernetesPodManager."""
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model=tests_per_model,
    )
    op.client = MagicMock()

    op.pod_manager  # noqa: B018 — access triggers cached_property creation

    mock_manager_cls.assert_called_once()
    extra = mock_manager_cls.call_args.kwargs["callback_extra_kwargs"]
    assert extra["tests_per_model"] is tests_per_model
    assert extra["test_results_per_model"] is op._test_results_per_model
    assert extra[CONTEXT_HOLDER_KEY] is op._context_holder


@patch("cosmos.operators._k8s_common._delete_xcom_backup_variable")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
def test_pod_manager_created_before_execute_sees_execution_context(mock_execute, mock_init, mock_delete):
    """pod_manager accessed before execute() must not hold a stale context=None (#2543 follow-up).

    callback_extra_kwargs captures the mutable context holder by reference,
    so the context set by execute() is visible to the manager no matter when
    it was created and the log callbacks can push model/test status XComs.
    """
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        tests_per_model={"model.pkg.orders": ["test.pkg.t1"]},
    )
    op.client = MagicMock()

    manager = op.pod_manager  # created before execute(): context not yet known
    assert manager._callback_extra_kwargs[CONTEXT_HOLDER_KEY][CONTEXT_KEY] is None

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}
    op.execute(context=context)

    assert op.pod_manager is manager  # still the same cached manager
    assert manager._callback_extra_kwargs[CONTEXT_HOLDER_KEY][CONTEXT_KEY] is context


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


# ---------------------------------------------------------------------------
# Per-model outlet emission (ExecutionMode.WATCHER_KUBERNETES)
#
# The producer builds an outlet URI map from the scheduler-side manifest and
# threads it (plus should_generate_model_uris) to the log-parsing callback; each
# consumer sensor then emits one Airflow Asset per URI on success. The map is
# pre-populated on the scheduler because the pod's manifest isn't reachable from
# there. See cosmos.operators._k8s_common.
# ---------------------------------------------------------------------------


def test_producer_initialises_outlet_state():
    """The producer defaults ``_should_generate_model_uris`` to True and initialises
    the (empty, mutable) outlet map and unset namespace at construction time."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    assert op._should_generate_model_uris is True
    assert op._model_outlet_uris == {}
    assert op._dataset_namespace is None
    assert op.manifest_filepath == ""


def test_producer_respects_should_generate_model_uris_flag():
    """``_should_generate_model_uris`` is wired explicitly by ``_add_watcher_producer_task``
    and must be honoured over the operator-level ``emit_datasets`` default."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        _should_generate_model_uris=False,
    )
    assert op._should_generate_model_uris is False


def test_producer_stores_manifest_filepath():
    """``manifest_filepath`` (threaded from ProjectConfig.manifest_path) is stored so
    execute() can read the manifest on the scheduler."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        manifest_filepath="/some/target/manifest.json",
    )
    assert op.manifest_filepath == "/some/target/manifest.json"


@patch("cosmos.operators._k8s_common.CosmosKubernetesPodManager")
def test_producer_pod_manager_forwards_outlet_state(mock_manager_cls):
    """pod_manager forwards the (by-reference) outlet map and the generation flag so the
    log-parsing callback can attach outlet URIs to each model's status XCom."""
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    op.client = MagicMock()

    op.pod_manager  # noqa: B018 — access triggers cached_property creation

    extra = mock_manager_cls.call_args.kwargs["callback_extra_kwargs"]
    assert extra["model_outlet_uris"] is op._model_outlet_uris
    assert extra["should_generate_model_uris"] is op._should_generate_model_uris


@patch("cosmos.dataset.compute_model_outlet_uris", return_value={"model.jaffle.a": ["postgres://h:5432/db.schema.a"]})
@patch("cosmos.dataset.get_dataset_namespace", return_value="postgres://h:5432")
def test_populate_model_outlet_uris_from_manifest(mock_namespace, mock_compute, tmp_path):
    """When enabled with a profile and an existing manifest, the producer resolves the
    namespace and fills the (same) outlet map in place from the manifest."""
    from cosmos.operators import _k8s_common

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=profile_config,
        image="dbt-image:latest",
        manifest_filepath=str(manifest),
    )
    outlet_map = op._model_outlet_uris  # capture the reference shared with the callback

    _k8s_common._populate_producer_model_outlet_uris(op)

    assert op._dataset_namespace == "postgres://h:5432"
    assert op._model_outlet_uris is outlet_map  # mutated in place, not reassigned
    assert op._model_outlet_uris == {"model.jaffle.a": ["postgres://h:5432/db.schema.a"]}
    # The manifest path is forwarded verbatim (never wrapped in Path) so remote schemes survive.
    mock_compute.assert_called_once_with(str(manifest), "postgres://h:5432")


@patch("cosmos.dataset.compute_model_outlet_uris")
def test_populate_model_outlet_uris_skips_when_disabled(mock_compute):
    """With ``_should_generate_model_uris=False`` the producer does no dataset work."""
    from cosmos.operators import _k8s_common

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=profile_config,
        image="dbt-image:latest",
        _should_generate_model_uris=False,
    )

    _k8s_common._populate_producer_model_outlet_uris(op)

    assert op._model_outlet_uris == {}
    assert op._dataset_namespace is None
    mock_compute.assert_not_called()


@patch("cosmos.dataset.compute_model_outlet_uris")
@patch("cosmos.dataset.get_dataset_namespace", return_value="postgres://h:5432")
def test_populate_model_outlet_uris_noop_without_manifest(mock_namespace, mock_compute):
    """A missing/empty manifest_filepath degrades to a no-op (dbt still runs; no datasets)."""
    from cosmos.operators import _k8s_common

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=profile_config,
        image="dbt-image:latest",
        manifest_filepath="",
    )

    _k8s_common._populate_producer_model_outlet_uris(op)

    assert op._model_outlet_uris == {}
    mock_compute.assert_not_called()


@patch("cosmos.dataset.get_dataset_namespace")
def test_populate_model_outlet_uris_noop_without_profile(mock_namespace):
    """Without a ProfileConfig the namespace can't be resolved, so emission is a no-op."""
    from cosmos.operators import _k8s_common

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        manifest_filepath="/some/manifest.json",
    )

    _k8s_common._populate_producer_model_outlet_uris(op)

    assert op._model_outlet_uris == {}
    assert op._dataset_namespace is None
    mock_namespace.assert_not_called()


@patch("cosmos.dataset.compute_model_outlet_uris", return_value={"model.jaffle.a": ["s3-uri"]})
@patch("cosmos.dataset.get_dataset_namespace", return_value="postgres://h:5432")
def test_populate_model_outlet_uris_forwards_remote_manifest_unchanged(mock_namespace, mock_compute):
    """A remote ``ObjectStoragePath`` manifest (e.g. ``s3://``) is passed to
    ``compute_model_outlet_uris`` unchanged, so its scheme is preserved (regression guard for
    wrapping it in ``Path`` which would mangle ``s3://b/m.json`` into ``s3:/b/m.json``)."""
    try:
        from airflow.sdk import ObjectStoragePath
    except ImportError:
        from airflow.io.path import ObjectStoragePath

    from cosmos.operators import _k8s_common

    remote_manifest = ObjectStoragePath("s3://my-bucket/target/manifest.json")
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=profile_config,
        image="dbt-image:latest",
        manifest_filepath=remote_manifest,
    )

    _k8s_common._populate_producer_model_outlet_uris(op)

    mock_compute.assert_called_once_with(remote_manifest, "postgres://h:5432")
    assert op._model_outlet_uris == {"model.jaffle.a": ["s3-uri"]}


@patch("cosmos.dataset.register_dataset_on_task")
def test_consumer_emits_datasets_on_success(mock_register):
    """Smoke test that the K8s consumer inherits BaseConsumerSensor dataset emission and that
    register_dataset_on_task works for this multiple-inheritance sensor (which does not inherit
    from the local operator). Full emit behaviour is covered in tests/operators/_watcher."""
    sensor = make_sensor()
    sensor.emit_datasets = True
    # Slash-delimited form so the URI is valid on both Airflow 2 and Airflow 3 (AIP-60).
    sensor._outlet_uris = ["postgres://h:5432/db/schema/stg_orders"]

    context = {"ti": MagicMock()}
    sensor._emit_datasets(context)

    mock_register.assert_called_once()
    args, _ = mock_register.call_args
    task_arg, inlets, outlets, ctx = args
    assert task_arg is sensor
    assert inlets == []
    assert [o.uri for o in outlets] == ["postgres://h:5432/db/schema/stg_orders"]
    assert ctx is context
