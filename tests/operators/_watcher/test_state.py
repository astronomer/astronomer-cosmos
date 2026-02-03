"""Unit tests for cosmos.operators._watcher.state module."""

from __future__ import annotations

import pytest

from cosmos.operators._watcher.state import (
    is_node_status_failed,
    is_node_status_success,
    is_node_status_terminal,
)


class TestNodeStatusHelpers:
    """Tests for node status helper functions."""

    @pytest.mark.parametrize("status", ["success", "pass"])
    def test_is_node_status_success_true(self, status: str):
        assert is_node_status_success(status) is True

    @pytest.mark.parametrize("status", ["failed", "fail", "error", "skipped", "warn", None, ""])
    def test_is_node_status_success_false(self, status: str | None):
        assert is_node_status_success(status) is False

    @pytest.mark.parametrize("status", ["failed", "fail", "error"])
    def test_is_node_status_failed_true(self, status: str):
        assert is_node_status_failed(status) is True

    @pytest.mark.parametrize("status", ["success", "pass", "skipped", "warn", None, ""])
    def test_is_node_status_failed_false(self, status: str | None):
        assert is_node_status_failed(status) is False

    @pytest.mark.parametrize("status", ["success", "pass", "failed", "fail", "error"])
    def test_is_node_status_terminal_true(self, status: str):
        assert is_node_status_terminal(status) is True

    @pytest.mark.parametrize("status", ["skipped", "warn", "running", None, ""])
    def test_is_node_status_terminal_false(self, status: str | None):
        assert is_node_status_terminal(status) is False
