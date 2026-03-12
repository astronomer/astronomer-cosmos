"""Unit tests for cosmos.operators._watcher.state module."""

from __future__ import annotations

import logging

import pytest

from cosmos.operators._watcher.state import (
    _log_dbt_event,
    is_dbt_node_status_failed,
    is_dbt_node_status_success,
    is_dbt_node_status_terminal,
)


class TestNodeStatusHelpers:
    """Tests for node status helper functions."""

    @pytest.mark.parametrize("status", ["success", "pass"])
    def test_is_dbt_node_status_success_true(self, status: str):
        assert is_dbt_node_status_success(status) is True

    @pytest.mark.parametrize("status", ["failed", "fail", "error", "skipped", "warn", None, ""])
    def test_is_dbt_node_status_success_false(self, status: str | None):
        assert is_dbt_node_status_success(status) is False

    @pytest.mark.parametrize("status", ["failed", "fail", "error"])
    def test_is_dbt_node_status_failed_true(self, status: str):
        assert is_dbt_node_status_failed(status) is True

    @pytest.mark.parametrize("status", ["success", "pass", "skipped", "warn", None, ""])
    def test_is_dbt_node_status_failed_false(self, status: str | None):
        assert is_dbt_node_status_failed(status) is False

    @pytest.mark.parametrize("status", ["success", "pass", "failed", "fail", "error"])
    def test_is_dbt_node_status_terminal_true(self, status: str):
        assert is_dbt_node_status_terminal(status) is True

    @pytest.mark.parametrize("status", ["skipped", "warn", "running", None, ""])
    def test_is_dbt_node_status_terminal_false(self, status: str | None):
        assert is_dbt_node_status_terminal(status) is False


@pytest.mark.parametrize(
    "dbt_event,expect_error,status",
    [
        (None, False, None),
        ("not_a_dict", False, None),
        ({}, False, None),  # empty dict returns early
        ({"status": "success", "msg": "ok"}, False, "success"),
        ({"status": "failed", "msg": "boom"}, True, "failed"),
        ({"status": "error", "msg": "boom"}, True, "error"),
        ({"status": None, "msg": "empty"}, True, "None"),
        ({"status": 123, "msg": "num"}, False, "123"),
    ],
)
def test_log_dbt_event(caplog, dbt_event, expect_error, status):
    caplog.set_level(logging.INFO)

    _log_dbt_event(dbt_event)

    # early return cases
    if not dbt_event or not isinstance(dbt_event, dict):
        assert len(caplog.records) == 0
        return

    messages = [record.getMessage() for record in caplog.records]

    assert any("Start:" in msg and "Finish:" in msg for msg in messages)

    error_logs = [r for r in caplog.records if r.levelno == logging.ERROR]

    if expect_error:
        assert len(error_logs) == 1
    else:
        assert len(error_logs) == 0

    if status:
        assert any(status in msg for msg in messages)
