"""Tests for the cosmos.debug module."""

from __future__ import annotations

import os
import time
from importlib import reload
from unittest.mock import MagicMock, patch

import pytest

from cosmos import debug, settings


class TestMemoryTracker:
    """Tests for the MemoryTracker class."""

    @pytest.fixture
    def reset_settings(self):
        """Reset settings after each test."""
        yield
        reload(settings)
        reload(debug)

    def test_memory_tracker_initialization(self):
        """Test MemoryTracker initializes with correct values."""
        tracker = debug.MemoryTracker(pid=os.getpid(), poll_interval=0.1)
        assert tracker.pid == os.getpid()
        assert tracker.poll_interval == 0.1
        assert tracker.max_rss_bytes == 0

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_memory_tracker_tracks_memory(self):
        """Test MemoryTracker actually tracks memory usage."""
        tracker = debug.MemoryTracker(pid=os.getpid(), poll_interval=0.05)
        tracker.start()
        # Give it time to sample
        time.sleep(0.2)
        tracker.stop()
        # Memory should be tracked (current process uses some memory)
        assert tracker.max_rss_bytes > 0

    def test_memory_tracker_stop_without_start(self):
        """Test MemoryTracker.stop() doesn't raise if not started."""
        tracker = debug.MemoryTracker(pid=os.getpid())
        # Should not raise
        tracker.stop()

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_memory_tracker_with_nonexistent_pid(self):
        """Test MemoryTracker handles non-existent PID gracefully."""
        # Use a very high PID that's unlikely to exist
        tracker = debug.MemoryTracker(pid=999999999, poll_interval=0.05)
        tracker.start()
        time.sleep(0.1)
        tracker.stop()
        # Should have 0 bytes since process doesn't exist
        assert tracker.max_rss_bytes == 0


class TestStartMemoryTracking:
    """Tests for the start_memory_tracking function."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow context."""
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.run_id = "test_run_123"
        return {"ti": mock_ti}

    def test_start_memory_tracking_disabled(self, mock_context):
        """Test start_memory_tracking does nothing when debug mode is disabled."""
        with patch.object(debug.settings, "enable_debug_mode", False):
            debug.start_memory_tracking(mock_context)
            # No tracker should be created
            task_key = f"{mock_context['ti'].dag_id}.{mock_context['ti'].task_id}.{mock_context['ti'].run_id}"
            assert task_key not in debug._memory_trackers

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_start_memory_tracking_enabled(self, mock_context):
        """Test start_memory_tracking creates tracker when debug mode is enabled."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            debug.start_memory_tracking(mock_context)
            task_key = f"{mock_context['ti'].dag_id}.{mock_context['ti'].task_id}.{mock_context['ti'].run_id}"
            assert task_key in debug._memory_trackers
            # Cleanup
            tracker = debug._memory_trackers.pop(task_key)
            tracker.stop()


class TestStopMemoryTracking:
    """Tests for the stop_memory_tracking function."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow context."""
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.run_id = "test_run_123"
        return {"ti": mock_ti}

    def test_stop_memory_tracking_disabled(self, mock_context):
        """Test stop_memory_tracking does nothing when debug mode is disabled."""
        with patch.object(debug.settings, "enable_debug_mode", False):
            debug.stop_memory_tracking(mock_context)
            # Should not raise and xcom_push should not be called
            mock_context["ti"].xcom_push.assert_not_called()

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_stop_memory_tracking_pushes_xcom(self, mock_context):
        """Test stop_memory_tracking pushes memory data to XCom."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            # Start tracking first
            debug.start_memory_tracking(mock_context)
            time.sleep(0.1)  # Let it sample
            # Stop and check XCom push
            debug.stop_memory_tracking(mock_context)
            mock_context["ti"].xcom_push.assert_called_once()
            call_args = mock_context["ti"].xcom_push.call_args
            assert call_args[1]["key"] == "cosmos_debug_max_memory_mb"
            assert isinstance(call_args[1]["value"], float)
            assert call_args[1]["value"] > 0

    def test_stop_memory_tracking_no_tracker(self, mock_context):
        """Test stop_memory_tracking handles missing tracker gracefully."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            # Don't start tracking, just stop
            debug.stop_memory_tracking(mock_context)
            # Should not raise and xcom_push should not be called
            mock_context["ti"].xcom_push.assert_not_called()


class TestIntegration:
    """Integration tests for the full debug flow."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow context."""
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.run_id = "test_run_integration"
        return {"ti": mock_ti}

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_full_tracking_lifecycle(self, mock_context):
        """Test complete memory tracking lifecycle."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            # Start
            debug.start_memory_tracking(mock_context)
            task_key = f"{mock_context['ti'].dag_id}.{mock_context['ti'].task_id}.{mock_context['ti'].run_id}"
            assert task_key in debug._memory_trackers

            # Simulate some work
            time.sleep(0.2)

            # Stop
            debug.stop_memory_tracking(mock_context)
            assert task_key not in debug._memory_trackers
            mock_context["ti"].xcom_push.assert_called_once()


class TestDbtLocalRunOperatorDebugIntegration:
    """Integration tests for DbtRunLocalOperator with debug mode enabled."""

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_dbt_run_local_operator_stores_memory_in_xcom_when_debug_enabled(self):
        """
        Test that DbtRunLocalOperator pushes peak memory utilization to XCom
        when debug mode is enabled.
        """
        from pathlib import Path

        from cosmos.config import ProfileConfig
        from cosmos.operators.local import DbtRunLocalOperator

        # Use the mini project for testing
        mini_project_dir = Path(__file__).parent / "sample" / "mini"
        mini_profile_path = mini_project_dir / "profiles.yml"

        profile_config = ProfileConfig(
            profile_name="mini",
            target_name="dev",
            profiles_yml_filepath=mini_profile_path,
        )

        operator = DbtRunLocalOperator(
            task_id="test_debug_memory",
            project_dir=str(mini_project_dir),
            profile_config=profile_config,
            emit_datasets=False,
        )

        # Create mock context
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_debug_memory"
        mock_ti.run_id = "test_run_debug"
        mock_context = {
            "ti": mock_ti,
            "run_id": "test_run_debug",
            "execution_date": MagicMock(),
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
            "ts": "2024-01-01T00:00:00+00:00",
            "ts_nodash": "20240101T000000",
            "ts_nodash_with_tz": "20240101T000000+0000",
            "prev_ds": None,
            "prev_ds_nodash": None,
            "next_ds": None,
            "next_ds_nodash": None,
            "yesterday_ds": "2023-12-31",
            "yesterday_ds_nodash": "20231231",
            "tomorrow_ds": "2024-01-02",
            "tomorrow_ds_nodash": "20240102",
            "prev_execution_date": None,
            "prev_execution_date_success": None,
            "next_execution_date": None,
            "dag": MagicMock(),
            "task": MagicMock(),
            "macros": MagicMock(),
            "params": {},
            "var": MagicMock(),
            "inlets": [],
            "outlets": [],
            "templates_dict": None,
            "conf": MagicMock(),
            "dag_run": MagicMock(),
            "test_mode": True,
            "outlet_events": MagicMock(),
        }

        # Patch settings to enable debug mode and mock the build_and_run_cmd to avoid actual dbt execution
        with (
            patch.object(settings, "enable_debug_mode", True),
            patch.object(operator, "build_and_run_cmd", return_value=None),
        ):
            operator.execute(mock_context)

        # Verify that xcom_push was called with the debug memory key
        xcom_calls = mock_ti.xcom_push.call_args_list
        memory_xcom_calls = [call for call in xcom_calls if call[1].get("key") == "cosmos_debug_max_memory_mb"]

        assert len(memory_xcom_calls) == 1, "Expected exactly one XCom push for cosmos_debug_max_memory_mb"
        memory_value = memory_xcom_calls[0][1]["value"]
        assert isinstance(memory_value, float), "Memory value should be a float"
        assert memory_value > 0, "Memory value should be greater than 0"

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_dbt_run_local_operator_does_not_store_memory_when_debug_disabled(self):
        """
        Test that DbtRunLocalOperator does NOT push memory utilization to XCom
        when debug mode is disabled (default behavior).
        """
        from pathlib import Path

        from cosmos.config import ProfileConfig
        from cosmos.operators.local import DbtRunLocalOperator

        # Use the mini project for testing
        mini_project_dir = Path(__file__).parent / "sample" / "mini"
        mini_profile_path = mini_project_dir / "profiles.yml"

        profile_config = ProfileConfig(
            profile_name="mini",
            target_name="dev",
            profiles_yml_filepath=mini_profile_path,
        )

        operator = DbtRunLocalOperator(
            task_id="test_debug_memory_disabled",
            project_dir=str(mini_project_dir),
            profile_config=profile_config,
            emit_datasets=False,
        )

        # Create mock context
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_debug_memory_disabled"
        mock_ti.run_id = "test_run_debug_disabled"
        mock_context = {
            "ti": mock_ti,
            "run_id": "test_run_debug_disabled",
            "execution_date": MagicMock(),
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
            "ts": "2024-01-01T00:00:00+00:00",
            "ts_nodash": "20240101T000000",
            "ts_nodash_with_tz": "20240101T000000+0000",
            "prev_ds": None,
            "prev_ds_nodash": None,
            "next_ds": None,
            "next_ds_nodash": None,
            "yesterday_ds": "2023-12-31",
            "yesterday_ds_nodash": "20231231",
            "tomorrow_ds": "2024-01-02",
            "tomorrow_ds_nodash": "20240102",
            "prev_execution_date": None,
            "prev_execution_date_success": None,
            "next_execution_date": None,
            "dag": MagicMock(),
            "task": MagicMock(),
            "macros": MagicMock(),
            "params": {},
            "var": MagicMock(),
            "inlets": [],
            "outlets": [],
            "templates_dict": None,
            "conf": MagicMock(),
            "dag_run": MagicMock(),
            "test_mode": True,
            "outlet_events": MagicMock(),
        }

        # Patch settings to disable debug mode (default) and mock build_and_run_cmd
        with (
            patch.object(settings, "enable_debug_mode", False),
            patch.object(operator, "build_and_run_cmd", return_value=None),
        ):
            operator.execute(mock_context)

        # Verify that xcom_push was NOT called with the debug memory key
        xcom_calls = mock_ti.xcom_push.call_args_list
        memory_xcom_calls = [call for call in xcom_calls if call[1].get("key") == "cosmos_debug_max_memory_mb"]

        assert (
            len(memory_xcom_calls) == 0
        ), "Expected no XCom push for cosmos_debug_max_memory_mb when debug is disabled"

    @pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
    def test_dbt_run_local_operator_stores_memory_even_on_failure(self):
        """
        Test that DbtRunLocalOperator pushes memory utilization to XCom
        even when the task execution fails.
        """
        from pathlib import Path

        from cosmos.config import ProfileConfig
        from cosmos.operators.local import DbtRunLocalOperator

        # Use the mini project for testing
        mini_project_dir = Path(__file__).parent / "sample" / "mini"
        mini_profile_path = mini_project_dir / "profiles.yml"

        profile_config = ProfileConfig(
            profile_name="mini",
            target_name="dev",
            profiles_yml_filepath=mini_profile_path,
        )

        operator = DbtRunLocalOperator(
            task_id="test_debug_memory_failure",
            project_dir=str(mini_project_dir),
            profile_config=profile_config,
            emit_datasets=False,
        )

        # Create mock context
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_debug_memory_failure"
        mock_ti.run_id = "test_run_debug_failure"
        mock_context = {
            "ti": mock_ti,
            "run_id": "test_run_debug_failure",
            "execution_date": MagicMock(),
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
            "ts": "2024-01-01T00:00:00+00:00",
            "ts_nodash": "20240101T000000",
            "ts_nodash_with_tz": "20240101T000000+0000",
            "prev_ds": None,
            "prev_ds_nodash": None,
            "next_ds": None,
            "next_ds_nodash": None,
            "yesterday_ds": "2023-12-31",
            "yesterday_ds_nodash": "20231231",
            "tomorrow_ds": "2024-01-02",
            "tomorrow_ds_nodash": "20240102",
            "prev_execution_date": None,
            "prev_execution_date_success": None,
            "next_execution_date": None,
            "dag": MagicMock(),
            "task": MagicMock(),
            "macros": MagicMock(),
            "params": {},
            "var": MagicMock(),
            "inlets": [],
            "outlets": [],
            "templates_dict": None,
            "conf": MagicMock(),
            "dag_run": MagicMock(),
            "test_mode": True,
            "outlet_events": MagicMock(),
        }

        # Patch settings to enable debug mode and mock build_and_run_cmd to raise an exception
        with (
            patch.object(settings, "enable_debug_mode", True),
            patch.object(operator, "build_and_run_cmd", side_effect=Exception("Simulated failure")),
        ):
            with pytest.raises(Exception, match="Simulated failure"):
                operator.execute(mock_context)

        # Verify that xcom_push was still called with the debug memory key
        xcom_calls = mock_ti.xcom_push.call_args_list
        memory_xcom_calls = [call for call in xcom_calls if call[1].get("key") == "cosmos_debug_max_memory_mb"]

        assert len(memory_xcom_calls) == 1, "Expected XCom push for cosmos_debug_max_memory_mb even on failure"
        memory_value = memory_xcom_calls[0][1]["value"]
        assert isinstance(memory_value, float), "Memory value should be a float"
        assert memory_value > 0, "Memory value should be greater than 0"
