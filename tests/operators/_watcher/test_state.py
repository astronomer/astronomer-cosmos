"""Unit tests for cosmos.operators._watcher.state module."""

from __future__ import annotations

import base64
import json
import logging
import zlib
from unittest.mock import MagicMock, patch

import pytest

from cosmos.operators._watcher.state import (
    _log_dbt_event,
    is_dbt_node_status_failed,
    is_dbt_node_status_skipped,
    is_dbt_node_status_success,
    is_dbt_node_status_terminal,
    is_producer_task_terminated,
    safe_xcom_push,
)
from cosmos.operators._watcher.xcom import (
    _backup_xcom_to_variable,
    _delete_xcom_backup_variable,
    _init_xcom_backup,
    _persist_backup,
    _restore_xcom_from_variable,
    _xcom_backup_variable_key,
)


class TestNodeStatusHelpers:
    """Tests for node status helper functions."""

    @pytest.mark.parametrize("status", ["success", "pass", "warn"])
    def test_is_dbt_node_status_success_true(self, status: str):
        assert is_dbt_node_status_success(status) is True

    @pytest.mark.parametrize("status", ["failed", "fail", "error", "skipped", None, ""])
    def test_is_dbt_node_status_success_false(self, status: str | None):
        assert is_dbt_node_status_success(status) is False

    @pytest.mark.parametrize("status", ["failed", "fail", "error"])
    def test_is_dbt_node_status_failed_true(self, status: str):
        assert is_dbt_node_status_failed(status) is True

    @pytest.mark.parametrize("status", ["success", "pass", "skipped", "warn", None, ""])
    def test_is_dbt_node_status_failed_false(self, status: str | None):
        assert is_dbt_node_status_failed(status) is False

    @pytest.mark.parametrize("status", ["skipped"])
    def test_is_dbt_node_status_skipped_true(self, status: str):
        assert is_dbt_node_status_skipped(status) is True

    @pytest.mark.parametrize("status", ["success", "pass", "failed", "fail", "error", "warn", None, ""])
    def test_is_dbt_node_status_skipped_false(self, status: str | None):
        assert is_dbt_node_status_skipped(status) is False

    @pytest.mark.parametrize("status", ["success", "pass", "warn", "failed", "fail", "error", "skipped"])
    def test_is_dbt_node_status_terminal_true(self, status: str):
        assert is_dbt_node_status_terminal(status) is True

    @pytest.mark.parametrize("status", ["running", None, ""])
    def test_is_dbt_node_status_terminal_false(self, status: str | None):
        assert is_dbt_node_status_terminal(status) is False


class TestProducerTaskTerminated:
    """Tests for is_producer_task_terminated helper."""

    @pytest.mark.parametrize("state", ["success", "failed", "skipped", "upstream_failed", "removed"])
    def test_terminal_states(self, state: str):
        assert is_producer_task_terminated(state) is True

    @pytest.mark.parametrize("state", ["running", "deferred", "queued", "scheduled", "up_for_reschedule", None, ""])
    def test_non_terminal_states(self, state: str | None):
        assert is_producer_task_terminated(state) is False


@pytest.mark.parametrize(
    "dbt_event,expect_error,status",
    [
        (None, False, None),
        ("not_a_dict", False, None),
        ({}, False, None),  # empty dict returns early
        ({"status": "success", "msg": "ok"}, False, "SUCCESS"),
        ({"status": "failed", "msg": "boom"}, True, "FAILED"),
        ({"status": "error", "msg": "boom"}, True, "ERROR"),
        ({"status": None, "msg": "empty"}, True, "NONE"),
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


class _MockTI:
    def __init__(self):
        self.store = {}
        self.dag_id = "test_dag"
        self.task_id = "test_task"

    def xcom_push(self, key, value, **_):
        self.store[key] = value


class TestXcomBackupVariableKey:
    """Tests for ``_xcom_backup_variable_key`` covering secrets-backend
    compatibility (AWS Secrets Manager and similar reject ``:``, ``+`` etc.)."""

    AWS_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

    def test_period_replacement_preserved(self):
        key = _xcom_backup_variable_key("a.b", "g.h", "r.s")
        assert key == "cosmos_xcom_backup__a___b__g__h__r_s"

    def test_run_id_with_colon_and_plus_is_sanitized(self):
        key = _xcom_backup_variable_key(
            dag_id="dbt_daily",
            task_group_id=None,
            run_id="scheduled__2026-05-04T10:15:00+00:00",
        )
        assert key == "cosmos_xcom_backup__dbt_daily__scheduled__2026-05-04T10_15_00_00_00"
        assert ":" not in key and "+" not in key

    @pytest.mark.parametrize(
        "dag_id,task_group_id,run_id",
        [
            ("dbt_daily", None, "scheduled__2026-05-04T10:15:00+00:00"),
            ("dag.with.dots", "group.id", "manual__2026-01-01T00:00:00+00:00"),
            ("dag with spaces", None, "manual__2026-01-01"),
            ("dag(parens)", None, "run/with/slashes"),
            ("dag*star", "group:colon", "run+plus"),
        ],
    )
    def test_result_contains_only_safe_characters(self, dag_id, task_group_id, run_id):
        key = _xcom_backup_variable_key(dag_id, task_group_id, run_id)
        assert all(c in self.AWS_ALLOWED for c in key), key


class TestInitXcomBackup:
    def test_sets_var_key_and_buffer_on_ti(self):
        ti = _MockTI()
        context = {"ti": ti, "run_id": "manual__2026-01-01"}

        _init_xcom_backup(context)

        assert isinstance(ti._cosmos_xcom_backup_var_key, str)
        assert "test_dag" in ti._cosmos_xcom_backup_var_key
        assert ti._cosmos_xcom_backup_buffer == {}

    def test_includes_task_group_id_when_present(self):
        ti = _MockTI()
        ti.task = MagicMock(task_group_id="my_group")
        context = {"ti": ti, "run_id": "manual__2026-01-01"}

        _init_xcom_backup(context)

        assert "my_group" in ti._cosmos_xcom_backup_var_key


class TestPersistBackup:
    @patch("airflow.models.Variable")
    def test_writes_compressed_data_to_variable(self, mock_variable):
        _persist_backup("my_var_key", {"key1": "value1", "key2": "value2"})

        mock_variable.set.assert_called_once()
        var_key, compressed = mock_variable.set.call_args[0]
        assert var_key == "my_var_key"
        data = json.loads(zlib.decompress(base64.b64decode(compressed.encode("utf-8"))).decode("utf-8"))
        assert data == {"key1": "value1", "key2": "value2"}

    @patch("airflow.models.Variable")
    def test_skips_empty_buffer(self, mock_variable):
        _persist_backup("my_var_key", {})

        mock_variable.set.assert_not_called()


class TestSafeXcomPushBackup:
    @patch("cosmos.operators._watcher.xcom._persist_backup")
    def test_accumulates_in_backup_buffer_when_active(self, mock_persist):
        ti = _MockTI()
        context = {"ti": ti, "run_id": "test_run"}
        _init_xcom_backup(context)

        safe_xcom_push(ti, "status_key", "success")

        assert ti.store["status_key"] == "success"
        assert ti._cosmos_xcom_backup_buffer["status_key"] == "success"
        mock_persist.assert_called_once()

    @patch("cosmos.operators._watcher.xcom._persist_backup")
    def test_does_not_backup_without_init(self, mock_persist):
        ti = _MockTI()

        safe_xcom_push(ti, "key", "value")

        assert ti.store["key"] == "value"
        mock_persist.assert_not_called()


class TestBackupXcomToVariable:
    @patch("cosmos.operators._watcher.xcom._persist_backup")
    def test_flushes_buffer(self, mock_persist):
        ti = _MockTI()
        context = {"ti": ti, "run_id": "test_run"}
        _init_xcom_backup(context)
        ti._cosmos_xcom_backup_buffer = {"k": "v"}

        _backup_xcom_to_variable(context)

        mock_persist.assert_called_once_with(ti._cosmos_xcom_backup_var_key, {"k": "v"})

    @patch("cosmos.operators._watcher.xcom._persist_backup")
    def test_noop_without_init(self, mock_persist):
        ti = _MockTI()
        context = {"ti": ti}

        _backup_xcom_to_variable(context)

        mock_persist.assert_not_called()


class TestDeleteXcomBackupVariable:
    @patch("airflow.models.Variable")
    def test_deletes_variable(self, mock_variable):
        ti = _MockTI()
        context = {"ti": ti, "run_id": "test_run"}
        _init_xcom_backup(context)

        _delete_xcom_backup_variable(context)

        mock_variable.delete.assert_called_once_with(ti._cosmos_xcom_backup_var_key)

    @patch("airflow.models.Variable")
    def test_noop_without_init(self, mock_variable):
        ti = _MockTI()
        context = {"ti": ti}

        _delete_xcom_backup_variable(context)

        mock_variable.delete.assert_not_called()

    @patch("airflow.models.Variable")
    def test_ignores_key_error(self, mock_variable):
        ti = _MockTI()
        context = {"ti": ti, "run_id": "test_run"}
        _init_xcom_backup(context)
        mock_variable.delete.side_effect = KeyError("not found")

        _delete_xcom_backup_variable(context)  # should not raise


class TestRestoreXcomFromVariable:
    @patch("cosmos.operators._watcher.xcom._persist_backup")
    @patch("airflow.models.Variable")
    def test_restores_entries(self, mock_variable, mock_persist):
        ti = _MockTI()
        backup = {"key1": "val1", "key2": "val2"}
        compressed = base64.b64encode(zlib.compress(json.dumps(backup).encode("utf-8"))).decode("utf-8")
        mock_variable.get.return_value = compressed
        context = {"ti": ti, "run_id": "test_run"}

        result = _restore_xcom_from_variable(context)

        assert result is True
        assert ti.store["key1"] == "val1"
        assert ti.store["key2"] == "val2"
        mock_variable.delete.assert_called_once()

    @patch("airflow.models.Variable")
    def test_returns_false_when_no_backup(self, mock_variable):
        ti = _MockTI()
        mock_variable.get.return_value = None
        context = {"ti": ti, "run_id": "test_run"}

        result = _restore_xcom_from_variable(context)

        assert result is False
