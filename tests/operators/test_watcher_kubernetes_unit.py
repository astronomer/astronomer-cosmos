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


class TestCallbacksNormalization:
    """Tests for the callbacks normalization logic in DbtProducerWatcherKubernetesOperator."""

    def test_callbacks_none_adds_watcher_callback(self):
        """
        Test that when callbacks is None, WatcherKubernetesCallback is added.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=None,
        )
        assert op.callbacks == [WatcherKubernetesCallback]

    def test_callbacks_not_provided_adds_watcher_callback(self):
        """
        Test that when callbacks is not provided, WatcherKubernetesCallback is added.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
        )
        assert op.callbacks == [WatcherKubernetesCallback]

    def test_callbacks_list_appends_watcher_callback(self):
        """
        Test that when callbacks is a list, WatcherKubernetesCallback is appended.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[CustomCallback],
        )
        assert op.callbacks == [CustomCallback, WatcherKubernetesCallback]

    def test_callbacks_tuple_appends_watcher_callback(self):
        """
        Test that when callbacks is a tuple, WatcherKubernetesCallback is appended.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=(CustomCallback,),
        )
        assert op.callbacks == [CustomCallback, WatcherKubernetesCallback]

    def test_callbacks_single_value_wraps_and_appends_watcher_callback(self):
        """
        Test that when callbacks is a single value (not list/tuple), it is wrapped in a list
        and WatcherKubernetesCallback is appended.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=CustomCallback,
        )
        assert op.callbacks == [CustomCallback, WatcherKubernetesCallback]

    def test_callbacks_empty_list_adds_watcher_callback(self):
        """
        Test that when callbacks is an empty list, WatcherKubernetesCallback is added.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[],
        )
        assert op.callbacks == [WatcherKubernetesCallback]

    def test_callbacks_multiple_values_appends_watcher_callback(self):
        """
        Test that when callbacks contains multiple values, WatcherKubernetesCallback is appended.
        """
        from cosmos.operators.watcher_kubernetes import WatcherKubernetesCallback

        class CustomCallback1:
            pass

        class CustomCallback2:
            pass

        op = DbtProducerWatcherKubernetesOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[CustomCallback1, CustomCallback2],
        )
        assert op.callbacks == [CustomCallback1, CustomCallback2, WatcherKubernetesCallback]


def test_callbacks_included_in_producer_operator():
    """
    Test that the WatcherKubernetesCallback is included in the callbacks of the DbtProducerWatcherKubernetesOperator.
    """
    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        callbacks=MagicMock,
    )
    callback_classes = [callback.__name__ for callback in op.callbacks]
    assert "WatcherKubernetesCallback" in callback_classes
    assert "MagicMock" in callback_classes

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        callbacks=[MagicMock],
    )
    callback_classes = [callback.__name__ for callback in op.callbacks]
    assert "WatcherKubernetesCallback" in callback_classes


# -----------------------------------------------------------------------------
# Outlet URI wiring: regression coverage for WATCHER_KUBERNETES dataset emission
#
# These tests exercise the two halves of the per-model dataset emission path:
#
#   producer execute()  ──► populates self._model_outlet_uris from the manifest
#                        └─► publishes outlets + namespace into module globals
#   progress_callback() ──► threads module globals into store_dbt_resource_status_from_log
#                        └─► XCom carries outlet_uris on per-node status
#   consumer execute() / execute_complete() ──► reads outlet_uris, emits Assets
#
# Without any one of these links the chain breaks and lineage catalogs show
# empty inputs/outputs for WATCHER_KUBERNETES pipelines.
# -----------------------------------------------------------------------------


from cosmos.operators import watcher_kubernetes as watcher_kubernetes_module


@pytest.fixture(autouse=True)
def reset_watcher_kubernetes_globals():
    """
    Reset the module-level producer-state globals between tests.

    These globals are the bridge between the producer operator's ``execute``
    and the static ``WatcherKubernetesCallback.progress_callback``. Tests that
    set them must not leak state into unrelated tests.
    """
    yield
    watcher_kubernetes_module.producer_task_context = None
    watcher_kubernetes_module.producer_task_model_outlet_uris = None
    watcher_kubernetes_module.producer_task_dataset_namespace = None


@patch("cosmos.operators.watcher_kubernetes._delete_xcom_backup_variable")
@patch("cosmos.operators.watcher_kubernetes._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
@patch("cosmos.operators.watcher_kubernetes.compute_model_outlet_uris")
@patch("cosmos.operators.watcher_kubernetes.get_dataset_namespace")
def test_producer_execute_populates_outlet_uris_and_globals(
    mock_get_namespace, mock_compute, mock_super_execute, mock_init, mock_delete, tmp_path
):
    """Producer ``execute`` must resolve namespace, read manifest, publish globals.

    Uses the module-level ``profile_config`` fixture (a real ProfileConfig pointing
    at the jaffle_shop profiles.yml) because ``execute`` guards the
    ``get_dataset_namespace`` call on ``profile_config is not None`` — passing
    ``None`` here would skip the resolver entirely and never exercise the
    populate path under test.
    """
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    mock_get_namespace.return_value = "bigquery"
    mock_compute.return_value = {"model.jaffle.a": ["bigquery/db.schema.a"]}

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=profile_config,
        image="dbt-image:latest",
        manifest_filepath=str(manifest),
    )

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    op.execute(context=context)

    assert op._dataset_namespace == "bigquery"
    assert op._model_outlet_uris == {"model.jaffle.a": ["bigquery/db.schema.a"]}
    assert watcher_kubernetes_module.producer_task_context is context
    assert watcher_kubernetes_module.producer_task_dataset_namespace == "bigquery"
    assert watcher_kubernetes_module.producer_task_model_outlet_uris is op._model_outlet_uris

    # compute_model_outlet_uris is always called with a Path; don't care about
    # str-vs-Path equality here — assert the content of the call instead.
    called_path, called_namespace = mock_compute.call_args[0]
    assert str(called_path) == str(manifest)
    assert called_namespace == "bigquery"


@patch("cosmos.operators.watcher_kubernetes._delete_xcom_backup_variable")
@patch("cosmos.operators.watcher_kubernetes._init_xcom_backup")
@patch("cosmos.operators.kubernetes.DbtBuildKubernetesOperator.execute")
@patch("cosmos.operators.watcher_kubernetes.compute_model_outlet_uris")
@patch("cosmos.operators.watcher_kubernetes.get_dataset_namespace")
def test_producer_execute_no_manifest_skips_silently(
    mock_get_namespace, mock_compute, mock_super_execute, mock_init, mock_delete
):
    """
    Missing manifest must not raise — dataset emission is optional, and dbt
    execution / status reporting must continue to work without it. Matches the
    SUBPROCESS watcher's graceful-degradation behaviour.
    """
    mock_get_namespace.return_value = "bigquery"

    op = DbtProducerWatcherKubernetesOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
    )
    # manifest_filepath defaults to ""

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}

    op.execute(context=context)

    assert op._model_outlet_uris == {}
    mock_compute.assert_not_called()


@patch("cosmos.operators.watcher_kubernetes.store_dbt_resource_status_from_log")
def test_progress_callback_passes_outlet_state_to_log_parser(mock_store):
    """
    ``progress_callback`` must forward the module-level outlet URI map and
    namespace into ``store_dbt_resource_status_from_log``. Without this, the
    parser writes empty ``outlet_uris`` to XCom regardless of what the
    producer resolved.
    """
    sentinel_uris: dict[str, list[str]] = {"model.x": ["bigquery/a.b.c"]}
    watcher_kubernetes_module.producer_task_context = {"run_id": "r"}
    watcher_kubernetes_module.producer_task_model_outlet_uris = sentinel_uris
    watcher_kubernetes_module.producer_task_dataset_namespace = "bigquery"

    watcher_kubernetes_module.WatcherKubernetesCallback.progress_callback(
        line="some log line",
        client=MagicMock(),
        mode="sync",
        container_name="base",
        timestamp=None,
        pod=MagicMock(),
    )

    mock_store.assert_called_once()
    _, call_kwargs = mock_store.call_args
    assert call_kwargs["model_outlet_uris"] is sentinel_uris
    assert call_kwargs["dataset_namespace"] == "bigquery"


@patch("cosmos.operators.watcher_kubernetes.register_dataset_on_task")
def test_consumer_emit_datasets_registers_assets(mock_register):
    """Consumer sensor emits one Asset per outlet URI when ``_outlet_uris`` is populated."""
    sensor = make_sensor(emit_datasets=True)
    sensor._outlet_uris = ["bigquery/db.schema.a", "bigquery/db.schema.b"]
    sensor.model_unique_id = "model.jaffle.a"

    context: dict = {"outlet_events": {}}
    sensor._emit_datasets(context)

    mock_register.assert_called_once()
    task_arg, inlets, outlets, _ctx = mock_register.call_args[0]
    assert task_arg is sensor
    assert inlets == []
    assert [outlet.uri for outlet in outlets] == ["bigquery/db.schema.a", "bigquery/db.schema.b"]


@patch("cosmos.operators.watcher_kubernetes.register_dataset_on_task")
def test_consumer_emit_datasets_noop_when_disabled_or_empty(mock_register):
    """Both no-op paths: ``emit_datasets=False`` and empty ``_outlet_uris``."""
    # Path 1: emit_datasets disabled — outlets present but ignored.
    sensor = make_sensor(emit_datasets=False)
    sensor.model_unique_id = "model.jaffle.a"
    sensor._outlet_uris = ["bigquery/db.schema.a"]
    sensor._emit_datasets({})

    # Path 2: emit_datasets enabled but producer resolved no outlets.
    sensor = make_sensor(emit_datasets=True)
    sensor.model_unique_id = "model.jaffle.a"
    sensor._outlet_uris = []
    sensor._emit_datasets({})

    mock_register.assert_not_called()


@patch("cosmos.operators.watcher_kubernetes.DbtConsumerWatcherKubernetesSensor._emit_datasets")
@patch("cosmos.operators._watcher.base.BaseConsumerSensor.execute_complete")
def test_consumer_execute_complete_extracts_outlet_uris_from_event(mock_super_complete, mock_emit):
    """``execute_complete`` must stash ``outlet_uris`` off the trigger event before emitting."""
    sensor = make_sensor()
    event = {"status": "success", "outlet_uris": ["bigquery/db.schema.a"]}

    sensor.execute_complete({}, event)

    assert sensor._outlet_uris == ["bigquery/db.schema.a"]
    mock_emit.assert_called_once()
