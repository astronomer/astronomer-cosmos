from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

# Skip whole module if google provider isn't installed
try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator  # noqa: F401
except Exception:  # pragma: no cover
    pytest.skip("Skipping: apache-airflow-providers-google not installed", allow_module_level=True)

import cosmos.operators.watcher_gke as watcher_gke_module
from cosmos.operators.watcher_gke import (
    DbtConsumerWatcherGkeSensor,
    DbtProducerWatcherGkeOperator,
    WatcherGkeCallback,
)

BASE_WATCHER_KWARGS = {
    "task_id": "producer",
    "project_id": "my-project",
    "location": "europe-west6",
    "cluster_name": "my-cluster",
    "namespace": "default",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def test_producer_sets_retries_to_zero():
    op = DbtProducerWatcherGkeOperator(**BASE_WATCHER_KWARGS)
    assert getattr(op, "retries", None) == 0


def test_producer_injects_callback_and_log_format_json():
    op = DbtProducerWatcherGkeOperator(**BASE_WATCHER_KWARGS)

    callbacks = getattr(op, "callbacks", None)
    assert callbacks is not None
    assert isinstance(callbacks, list)
    assert WatcherGkeCallback in callbacks

    assert "--log-format" in op.dbt_cmd_flags
    assert "json" in op.dbt_cmd_flags


@patch("cosmos.operators.watcher_gke.CosmosKubernetesPodManager")
def test_producer_pod_manager_uses_callbacks(MockPodManager):
    op = DbtProducerWatcherGkeOperator(**BASE_WATCHER_KWARGS)

    # prevent GKEStartPodOperator from trying to create a real kube client
    object.__setattr__(op, "client", Mock())

    _ = op.pod_manager

    MockPodManager.assert_called_once()
    _, kwargs = MockPodManager.call_args
    assert "callbacks" in kwargs
    assert WatcherGkeCallback in kwargs["callbacks"]


@patch("cosmos.operators.gke.GKEStartPodOperator.execute", return_value="ok")
def test_producer_execute_sets_global_context(mock_execute):
    op = DbtProducerWatcherGkeOperator(**BASE_WATCHER_KWARGS)

    ti = Mock()
    ti.try_number = 1
    context = {"ti": ti, "task_instance": ti}

    watcher_gke_module.producer_task_context = None

    result = op.execute(context)

    assert watcher_gke_module.producer_task_context is context

    assert result is None

    mock_execute.assert_called_once()


@patch("cosmos.operators.gke.GKEStartPodOperator.execute")
def test_producer_execute_skips_on_retry(mock_execute):
    op = DbtProducerWatcherGkeOperator(**BASE_WATCHER_KWARGS)

    ti = Mock()
    ti.try_number = 2
    context = {"ti": ti, "task_instance": ti}

    result = op.execute(context)

    assert result is None
    mock_execute.assert_not_called()


def test_watcher_callback_injects_context_and_calls_store():
    fake_context = {"some": "ctx"}
    watcher_gke_module.producer_task_context = fake_context

    with patch("cosmos.operators.watcher_gke.store_dbt_resource_status_from_log") as store:
        WatcherGkeCallback.progress_callback(
            line="hello",
            client=Mock(),
            mode="x",
            container_name="base",
            timestamp=None,
            pod=Mock(),
        )

        store.assert_called_once()
        called_kwargs = store.call_args.args[1]
        assert called_kwargs["context"] is fake_context


def test_consumer_use_event_false():
    class Dummy(DbtConsumerWatcherGkeSensor):
        pass

    dummy = object.__new__(Dummy)
    assert Dummy.use_event(dummy) is False
