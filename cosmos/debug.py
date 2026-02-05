"""
Debug utilities for Cosmos.

When debug mode is enabled via the `enable_debug_mode` setting, Cosmos will track
memory utilization during task execution and push the maximum memory usage to XCom.
"""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from cosmos import settings
from cosmos.log import get_logger

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

logger = get_logger(__name__)

# Global dictionary to store memory trackers per task
_memory_trackers: dict[str, MemoryTracker] = {}


class MemoryTracker:
    """
    Tracks maximum RSS memory (bytes) for a process and all of its children.
    Sampling-based to work across Airflow 2 & 3 without executor internals.
    """

    def __init__(self, pid: int, poll_interval: float = 0.5):
        self.pid = pid
        self.poll_interval = poll_interval
        self.max_rss_bytes = 0
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the memory tracking thread."""
        self._thread.start()

    def stop(self) -> None:
        """Stop the memory tracking thread."""
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def _run(self) -> None:
        """Background thread that polls memory usage."""
        if not PSUTIL_AVAILABLE:
            return

        try:
            parent = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return

        while not self._stop_event.is_set():
            rss = 0
            try:
                processes = [parent] + parent.children(recursive=True)
                for p in processes:
                    try:
                        rss += p.memory_info().rss
                    except psutil.NoSuchProcess:
                        continue
                self.max_rss_bytes = max(self.max_rss_bytes, rss)
            except psutil.NoSuchProcess:
                break

            time.sleep(self.poll_interval)


def start_memory_tracking(context: Context) -> None:
    """
    Callback to start memory tracking for a task.

    This function should be used as an `on_execute_callback` for Cosmos operators
    when debug mode is enabled.

    :param context: The Airflow task context.
    """
    if not settings.enable_debug_mode:
        return

    if not PSUTIL_AVAILABLE:
        logger.warning(
            "psutil is not available. Memory tracking is disabled. Install psutil to enable memory tracking."
        )
        return

    ti = context["ti"]
    task_key = f"{ti.dag_id}.{ti.task_id}.{ti.run_id}"
    pid = os.getpid()
    tracker = MemoryTracker(pid=pid, poll_interval=settings.debug_memory_poll_interval_seconds)
    _memory_trackers[task_key] = tracker
    tracker.start()
    logger.debug("Started memory tracking for task %s (PID: %s)", task_key, pid)


def stop_memory_tracking(context: Context) -> None:
    """
    Callback to stop memory tracking for a task and push the result to XCom.

    This function should be used as an `on_success_callback` or `on_failure_callback`
    for Cosmos operators when debug mode is enabled.

    :param context: The Airflow task context.
    """
    if not settings.enable_debug_mode:
        return

    if not PSUTIL_AVAILABLE:
        return

    ti = context["ti"]
    task_key = f"{ti.dag_id}.{ti.task_id}.{ti.run_id}"
    tracker = _memory_trackers.pop(task_key, None)

    if tracker:
        tracker.stop()
        max_mb = tracker.max_rss_bytes / 1024 / 1024
        logger.info("Max memory usage (RSS, incl. children): %.2f MB", max_mb)
        # Persist to XCom for observability
        ti.xcom_push(key="cosmos_debug_max_memory_mb", value=round(max_mb, 2))
