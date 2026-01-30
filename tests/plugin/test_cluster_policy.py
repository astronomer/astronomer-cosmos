from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from cosmos.operators._watcher.base import BaseConsumerSensor
from cosmos.plugin.cluster_policy import _is_watcher_sensor, task_instance_mutation_hook

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore[no-redef]


class TestIsWatcherSensor:
    """Tests for the _is_watcher_sensor helper function."""

    def test_is_watcher_sensor_returns_true_for_consumer_sensor(self):
        """Test that _is_watcher_sensor returns True for a BaseConsumerSensor instance."""
        mock_sensor = MagicMock(spec=BaseConsumerSensor)
        task_instance = SimpleNamespace(task=mock_sensor)

        result = _is_watcher_sensor(task_instance)

        assert result is True

    def test_is_watcher_sensor_returns_false_for_non_sensor(self):
        """Test that _is_watcher_sensor returns False for non-BaseConsumerSensor tasks."""
        mock_operator = MagicMock(spec=EmptyOperator)
        task_instance = SimpleNamespace(task=mock_operator)

        result = _is_watcher_sensor(task_instance)

        assert result is False


class TestTaskInstanceMutationHook:
    """Tests for the task_instance_mutation_hook cluster policy."""

    @pytest.mark.parametrize(
        "airflow_version,try_number,should_set_queue",
        [
            # Airflow 3.x: retry_number is 1, queue set on try_number >= 1
            (Version("3.0.0"), 0, False),  # try_number 0 should not set queue
            (Version("3.0.0"), 1, True),  # try_number 1 should set queue (first retry)
            (Version("3.0.0"), 2, True),  # try_number 2 should set queue
            (Version("3.1.0"), 1, True),  # Airflow 3.1.0 should also work
            # Airflow 2.x: retry_number is 2, queue set on try_number >= 2
            (Version("2.6.0"), 1, False),  # try_number 1 (first attempt) should not set queue
            (Version("2.6.0"), 2, True),  # try_number 2 (first retry) should set queue
            (Version("2.6.0"), 3, True),  # try_number 3 should set queue
            (Version("2.10.0"), 2, True),  # Later Airflow 2.x versions should also work
        ],
    )
    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    def test_queue_set_based_on_airflow_version_and_try_number(self, airflow_version, try_number, should_set_queue):
        """Test that queue is set correctly based on Airflow version and try_number."""
        with patch("cosmos.plugin.cluster_policy.AIRFLOW_VERSION", airflow_version):
            mock_sensor = MagicMock(spec=BaseConsumerSensor)
            task_instance = SimpleNamespace(
                task=mock_sensor,
                task_id="test_task",
                try_number=try_number,
                queue="default",
            )

            task_instance_mutation_hook(task_instance)

            if should_set_queue:
                assert task_instance.queue == "custom_retry_queue"
            else:
                assert task_instance.queue == "default"

    @pytest.mark.parametrize("nullish_value", [None, 0, ""])
    def test_queue_not_set_when_watcher_retry_queue_is_none(self, nullish_value):
        """Test that queue is not modified when watcher_retry_queue setting is None."""
        with patch("cosmos.plugin.cluster_policy.watcher_retry_queue", nullish_value):
            mock_sensor = MagicMock(spec=BaseConsumerSensor)
            task_instance = SimpleNamespace(
                task=mock_sensor,
                task_id="test_task",
                try_number=2,
                queue="default",
            )

            task_instance_mutation_hook(task_instance)

            assert task_instance.queue == "default"

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    def test_queue_not_set_for_non_watcher_sensor(self):
        """Test that queue is not modified for tasks that are not watcher sensors."""
        mock_operator = MagicMock(spec=EmptyOperator)
        task_instance = SimpleNamespace(
            task=mock_operator,
            task_id="test_task",
            try_number=2,
            queue="default",
        )

        task_instance_mutation_hook(task_instance)

        assert task_instance.queue == "default"

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    def test_queue_not_set_when_try_number_is_none(self):
        """Test that queue is not modified when try_number is None."""
        mock_sensor = MagicMock(spec=BaseConsumerSensor)
        task_instance = SimpleNamespace(
            task=mock_sensor,
            task_id="test_task",
            try_number=None,
            queue="default",
        )

        task_instance_mutation_hook(task_instance)

        assert task_instance.queue == "default"

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    def test_queue_not_set_when_try_number_is_zero(self):
        """Test that queue is not modified when try_number is 0."""
        mock_sensor = MagicMock(spec=BaseConsumerSensor)
        task_instance = SimpleNamespace(
            task=mock_sensor,
            task_id="test_task",
            try_number=0,
            queue="default",
        )

        task_instance_mutation_hook(task_instance)

        assert task_instance.queue == "default"

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    @patch("cosmos.plugin.cluster_policy.log")
    def test_logging_when_queue_is_set(self, mock_log):
        """Test that appropriate log message is generated when queue is set."""
        mock_sensor = MagicMock(spec=BaseConsumerSensor)
        task_instance = SimpleNamespace(
            task=mock_sensor,
            task_id="my_test_task",
            try_number=2,
            queue="default",
        )

        task_instance_mutation_hook(task_instance)

        mock_log.info.assert_called_once_with(
            "Setting task my_test_task to use watcher retry queue: custom_retry_queue",
        )

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "custom_retry_queue")
    @patch("cosmos.plugin.cluster_policy.log")
    def test_no_logging_when_queue_not_set(self, mock_log):
        """Test that no log message is generated when queue is not set."""
        mock_operator = MagicMock(spec=EmptyOperator)
        task_instance = SimpleNamespace(
            task=mock_operator,
            task_id="my_test_task",
            try_number=2,
            queue="default",
        )

        task_instance_mutation_hook(task_instance)

        mock_log.info.assert_not_called()

    @pytest.mark.parametrize(
        "queue_name",
        [
            "custom_queue",
            "high_memory_queue",
            "retry-queue-123",
            "kubernetes-queue",
        ],
    )
    @patch("cosmos.plugin.cluster_policy.AIRFLOW_VERSION", AIRFLOW_VERSION)
    def test_various_queue_names(self, queue_name):
        """Test that different queue names are correctly applied."""
        with patch("cosmos.plugin.cluster_policy.watcher_retry_queue", queue_name):
            mock_sensor = MagicMock(spec=BaseConsumerSensor)
            task_instance = SimpleNamespace(
                task=mock_sensor,
                task_id="test_task",
                try_number=2,
                queue="default",
            )

            task_instance_mutation_hook(task_instance)

            assert task_instance.queue == queue_name

    @patch("cosmos.plugin.cluster_policy.watcher_retry_queue", "retry_queue")
    @patch("cosmos.plugin.cluster_policy.AIRFLOW_VERSION", AIRFLOW_VERSION)
    def test_queue_already_set_gets_overwritten(self):
        """Test that the queue is overwritten even if it was previously set to a different value."""
        mock_sensor = MagicMock(spec=BaseConsumerSensor)
        task_instance = SimpleNamespace(
            task=mock_sensor,
            task_id="test_task",
            try_number=2,
            queue="some_other_queue",
        )

        task_instance_mutation_hook(task_instance)

        assert task_instance.queue == "retry_queue"
