import logging
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from packaging.version import Version

from cosmos.constants import _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION

if Version(airflow_k8s_provider_version) < _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION:
    pytest.skip(
        f"Watcher GCP GKE depends on apache-airflow-providers-cncf-kubernetes >= {_K8s_WATCHER_MIN_K8S_PROVIDER_VERSION}. Current version: {airflow_k8s_provider_version} ",
        allow_module_level=True,
    )

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator  # noqa: F401
except ImportError:
    pytest.skip("Google Cloud provider not installed", allow_module_level=True)

from cosmos.operators.watcher_gcp_gke import (
    DbtBuildWatcherGcpGkeOperator,
    DbtConsumerWatcherGcpGkeSensor,
    DbtProducerWatcherGcpGkeOperator,
)


GKE_KWARGS = {
    "project_id": "my-gcp-project",
    "location": "us-central1",
    "cluster_name": "my-gke-cluster",
}


def test_retries_set_to_zero_on_init():
    """
    Test that the operator sets retries to 0 during initialization.
    """
    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        **GKE_KWARGS,
    )
    assert op.retries == 0


def test_retries_overridden_even_if_user_sets_them():
    """
    Test that even if a user explicitly sets retries, they are overridden to 0.
    """
    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        retries=5,
        **GKE_KWARGS,
    )
    assert op.retries == 0


@patch("cosmos.operators.gcp_gke.DbtBuildGcpGkeOperator.execute")
def test_skips_retry_attempt(mock_execute, caplog):
    """
    Test that the operator skips execution when a retry is attempted (try_number > 1).
    """
    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        **GKE_KWARGS,
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
    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        **GKE_KWARGS,
    )

    context = {"ti": None}

    with pytest.raises(AirflowException) as excinfo:
        op.execute(context=context)

    assert "expects a task instance" in str(excinfo.value)


def test_dbt_build_watcher_gcp_gke_operator_raises_not_implemented_error():
    expected_message = (
        "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
        "since the build command is executed by the producer task."
    )

    with pytest.raises(NotImplementedError, match=expected_message):
        DbtBuildWatcherGcpGkeOperator()


def make_sensor(**kwargs):
    extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
    kwargs["extra_context"] = extra_context
    sensor = DbtConsumerWatcherGcpGkeSensor(
        task_id="model.my_model",
        project_dir="/tmp/project",
        profile_config=None,
        deferrable=False,
        image="dbt-image:latest",
        **GKE_KWARGS,
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
    ti.xcom_pull.return_value = "success"
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    ti.xcom_pull.assert_called()


@patch("cosmos.operators.gcp_gke.DbtGcpGkeBaseOperator.build_and_run_cmd")
def test_retry_executes_as_dbt_run_gcp_gke_operator(mock_build_and_run_cmd):
    """
    On retry (try_number > 1), the sensor should fall back to executing
    as DbtRunGcpGkeOperator by calling build_and_run_cmd.
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
    DbtConsumerWatcherGcpGkeSensor should return False for use_event(),
    meaning it uses XCom-based status retrieval instead of events.
    """
    sensor = make_sensor()
    assert sensor.use_event() is False


class TestCallbacksNormalization:
    """Tests for the callbacks normalization logic in DbtProducerWatcherGcpGkeOperator."""

    def test_callbacks_none_adds_watcher_callback(self):
        """
        Test that when callbacks is None, WatcherGcpGkeCallback is added.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=None,
            **GKE_KWARGS,
        )
        assert op.callbacks == [WatcherGcpGkeCallback]

    def test_callbacks_not_provided_adds_watcher_callback(self):
        """
        Test that when callbacks is not provided, WatcherGcpGkeCallback is added.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            **GKE_KWARGS,
        )
        assert op.callbacks == [WatcherGcpGkeCallback]

    def test_callbacks_list_appends_watcher_callback(self):
        """
        Test that when callbacks is a list, WatcherGcpGkeCallback is appended.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[CustomCallback],
            **GKE_KWARGS,
        )
        assert op.callbacks == [CustomCallback, WatcherGcpGkeCallback]

    def test_callbacks_tuple_appends_watcher_callback(self):
        """
        Test that when callbacks is a tuple, WatcherGcpGkeCallback is appended.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=(CustomCallback,),
            **GKE_KWARGS,
        )
        assert op.callbacks == [CustomCallback, WatcherGcpGkeCallback]

    def test_callbacks_single_value_wraps_and_appends_watcher_callback(self):
        """
        Test that when callbacks is a single value (not list/tuple), it is wrapped in a list
        and WatcherGcpGkeCallback is appended.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        class CustomCallback:
            pass

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=CustomCallback,
            **GKE_KWARGS,
        )
        assert op.callbacks == [CustomCallback, WatcherGcpGkeCallback]

    def test_callbacks_empty_list_adds_watcher_callback(self):
        """
        Test that when callbacks is an empty list, WatcherGcpGkeCallback is added.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[],
            **GKE_KWARGS,
        )
        assert op.callbacks == [WatcherGcpGkeCallback]

    def test_callbacks_multiple_values_appends_watcher_callback(self):
        """
        Test that when callbacks contains multiple values, WatcherGcpGkeCallback is appended.
        """
        from cosmos.operators.watcher_gcp_gke import WatcherGcpGkeCallback

        class CustomCallback1:
            pass

        class CustomCallback2:
            pass

        op = DbtProducerWatcherGcpGkeOperator(
            project_dir=".",
            profile_config=None,
            image="dbt-image:latest",
            callbacks=[CustomCallback1, CustomCallback2],
            **GKE_KWARGS,
        )
        assert op.callbacks == [CustomCallback1, CustomCallback2, WatcherGcpGkeCallback]


def test_callbacks_included_in_producer_operator():
    """
    Test that the WatcherGcpGkeCallback is included in the callbacks of the DbtProducerWatcherGcpGkeOperator.
    """
    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        callbacks=MagicMock,
        **GKE_KWARGS,
    )
    callback_classes = [callback.__name__ for callback in op.callbacks]
    assert "WatcherGcpGkeCallback" in callback_classes
    assert "MagicMock" in callback_classes

    op = DbtProducerWatcherGcpGkeOperator(
        project_dir=".",
        profile_config=None,
        image="dbt-image:latest",
        callbacks=[MagicMock],
        **GKE_KWARGS,
    )
    callback_classes = [callback.__name__ for callback in op.callbacks]
    assert "WatcherGcpGkeCallback" in callback_classes
