"""Tests for the cosmos.debug module."""

from __future__ import annotations

import os
import time
from datetime import datetime
from importlib import reload
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG

from cosmos import debug, settings
from cosmos.config import ProfileConfig
from cosmos.operators.local import DbtRunLocalOperator
from tests.utils import test_dag as run_test_dag


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


class TestPsutilNotAvailable:
    """Tests for when psutil is not available."""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow context."""
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.run_id = "test_run_no_psutil"
        return {"ti": mock_ti}

    def test_memory_tracker_run_without_psutil(self):
        """Test MemoryTracker._run() returns early when psutil is not available."""
        tracker = debug.MemoryTracker(pid=os.getpid(), poll_interval=0.05)
        with patch.object(debug, "PSUTIL_AVAILABLE", False):
            # Call _run directly to test the branch
            tracker._run()
            # Should return immediately without tracking any memory
            assert tracker.max_rss_bytes == 0

    def test_start_memory_tracking_without_psutil_logs_warning(self, mock_context):
        """Test start_memory_tracking logs warning when psutil is not available."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            with patch.object(debug, "PSUTIL_AVAILABLE", False):
                with patch.object(debug.logger, "warning") as mock_warning:
                    debug.start_memory_tracking(mock_context)
                    mock_warning.assert_called_once()
                    assert "psutil is not available" in mock_warning.call_args[0][0]
                    # No tracker should be created
                    task_key = f"{mock_context['ti'].dag_id}.{mock_context['ti'].task_id}.{mock_context['ti'].run_id}"
                    assert task_key not in debug._memory_trackers

    def test_stop_memory_tracking_without_psutil_returns_early(self, mock_context):
        """Test stop_memory_tracking returns early when psutil is not available."""
        with patch.object(debug.settings, "enable_debug_mode", True):
            with patch.object(debug, "PSUTIL_AVAILABLE", False):
                debug.stop_memory_tracking(mock_context)
                # Should not push to XCom
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


MINI_DBT_PROJ_DIR = Path(__file__).parent / "sample" / "mini"
MINI_DBT_PROJ_PROFILE = MINI_DBT_PROJ_DIR / "profiles.yml"

mini_profile_config = ProfileConfig(
    profile_name="mini",
    target_name="dev",
    profiles_yml_filepath=MINI_DBT_PROJ_PROFILE,
)


@pytest.mark.integration
@pytest.mark.skipif(not debug.PSUTIL_AVAILABLE, reason="psutil not available")
def test_dbt_run_local_operator_stores_memory_in_xcom_when_debug_enabled():
    """
    Integration test that DbtRunLocalOperator pushes peak memory utilization to XCom
    when debug mode is enabled.
    """
    with patch.object(settings, "enable_debug_mode", True):
        with DAG("test-debug-memory", start_date=datetime(2022, 1, 1)) as dag:
            run_operator = DbtRunLocalOperator(
                profile_config=mini_profile_config,
                project_dir=MINI_DBT_PROJ_DIR,
                task_id="run",
                append_env=True,
                emit_datasets=False,
            )
            run_operator

        dag_run = run_test_dag(dag)

        # Get the task instance to check XCom
        ti = dag_run.get_task_instance(task_id="run")
        memory_value = ti.xcom_pull(key="cosmos_debug_max_memory_mb")

        assert memory_value is not None, "Expected cosmos_debug_max_memory_mb in XCom"
        assert isinstance(memory_value, float), "Memory value should be a float"
        assert memory_value > 0, "Memory value should be greater than 0"
