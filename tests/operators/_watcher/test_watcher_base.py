from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException

from cosmos.operators._watcher.base import (
    BaseConsumerSensor,
    _accumulate_dbt_log_event,
    _flush_dbt_event,
    _flush_dbt_event_on_error,
    _process_dbt_log_event,
    store_dbt_resource_status_from_log,
)
from cosmos.operators.local import DbtRunLocalOperator


class TestBaseConsumerSensor:

    def test_extra_context_is_stored_on_instance(self):
        """Consumer sensor stores extra_context so it is available at runtime."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}, "run_id": "run_123"}
        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context=extra_context,
        )
        assert sensor.extra_context == extra_context
        assert sensor.model_unique_id == "model.jaffle_shop.stg_orders"

    def test_extra_context_defaults_to_empty_dict_when_not_passed(self):
        """When extra_context is not in kwargs, sensor.extra_context is {}."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
        )
        assert sensor.extra_context == {}

    @pytest.mark.parametrize(
        "event_name,should_push",
        [
            (None, False),
            ("LogStartLine", False),
            ("NodeFinished", True),
            ("NodeStart", True),
        ],
    )
    def test_process_dbt_log_event_only_pushes_when_event_in_allowlist(self, event_name, should_push):
        """Only dbt events whose names are in _DBT_EVENT_ALLOWLIST are pushed to XCom."""
        task_instance = Mock()

        dbt_log = {
            "data": {
                "node_info": {
                    "unique_id": "model.test.my_model",
                    "node_status": "success",
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "msg": "model finished",
            },
            "info": {"name": event_name} if event_name is not None else {},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)

            if should_push:
                mock_push.assert_called_once()
                call_kwargs = mock_push.call_args.kwargs
                assert call_kwargs["key"] == "model__test__my_model_dbt_event"
                assert call_kwargs["value"]["status"] == "success"
                assert call_kwargs["value"]["msg"] == "model finished"
            else:
                mock_push.assert_not_called()

    def test_process_dbt_log_event_skips_when_no_unique_id(self):
        """Events with no node_info.unique_id are not pushed."""
        task_instance = Mock()

        dbt_log = {
            "data": {"node_info": {}, "msg": "some log"},
            "info": {"name": "NodeFinished"},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)
            mock_push.assert_not_called()

    def test_process_dbt_log_event_captures_error_from_run_result_message(self):
        """dbt-runner mode: the error text lives in NodeFinished.data.run_result.message (data.msg/info.msg
        are empty there). It must reach the consumer's _dbt_event."""
        task_instance = Mock()
        dbt_log = {
            "data": {
                "node_info": {
                    "unique_id": "model.test.bad_model",
                    "node_status": "error",
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "run_result": {"status": "error", "message": "Runtime Error ... Catalog Error: Table does not exist!"},
                "msg": "",
            },
            "info": {"name": "NodeFinished", "msg": ""},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _process_dbt_log_event(task_instance, dbt_log)

        value = mock_push.call_args.kwargs["value"]
        assert value["status"] == "error"
        assert value["msg"] == "Runtime Error ... Catalog Error: Table does not exist!"

    def test_execute_complete_raises_airflow_skip_exception_when_status_is_skipped(self):
        """execute_complete raises AirflowSkipException when the trigger sends status='skipped'."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        context = {"ti": Mock()}
        with pytest.raises(AirflowSkipException, match="was skipped by the dbt command"):
            sensor.execute_complete(context, {"status": "skipped", "reason": "source_not_fresh"})

    def test_execute_complete_logs_dbt_event_on_success(self):
        """Deferrable path: execute_complete logs the per-node dbt event once on success, not only on failure.

        Regression for #2456 - removing the per-poll trigger log must not drop the single terminal log line."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        context = {"ti": Mock()}
        with (
            patch("cosmos.operators._watcher.base.get_xcom_val", return_value={"status": "success", "msg": "ok"}),
            patch("cosmos.operators._watcher.base._log_dbt_event") as mock_log,
        ):
            sensor.execute_complete(context, {"status": "success"})
        mock_log.assert_called_once()

    def test_poke_raises_airflow_skip_exception_when_status_is_skipped(self):
        """poke raises AirflowSkipException when node status is 'skipped'."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        mock_ti = Mock()
        mock_ti.try_number = 1
        context = {"ti": mock_ti, "run_id": "run_123"}

        with (
            patch.object(sensor, "_get_producer_task_status", return_value="running"),
            patch.object(sensor, "_get_node_status", return_value="skipped"),
            patch.object(sensor, "_log_startup_events"),
        ):
            with pytest.raises(AirflowSkipException, match="was skipped by the dbt command"):
                sensor.poke(context)

    def test_poke_logs_dbt_event_only_at_terminal(self):
        """poke must not log the per-node dbt event while the node is still running (status None); it logs
        once the node is terminal. Regression for #2456 on the non-deferrable path."""

        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context={"dbt_node_config": {"unique_id": "model.pkg.my_model"}},
        )
        mock_ti = Mock()
        mock_ti.try_number = 1
        context = {"ti": mock_ti, "run_id": "run_123"}

        with (
            patch.object(sensor, "_get_producer_task_status", return_value="running"),
            patch.object(sensor, "_log_startup_events"),
            patch.object(sensor, "_cache_compiled_sql"),
            patch("cosmos.operators._watcher.base.get_xcom_val", return_value={"status": "success", "msg": "ok"}),
            patch("cosmos.operators._watcher.base._log_dbt_event") as mock_log,
        ):
            # Node still running: must not log (it would duplicate on each poke).
            with (
                patch.object(sensor, "_get_node_status", return_value=None),
                patch.object(sensor, "_handle_no_dbt_node_status", return_value=False),
            ):
                assert sensor.poke(context) is False
            mock_log.assert_not_called()

            # Node terminal: logs exactly once.
            with patch.object(sensor, "_get_node_status", return_value="success"):
                assert sensor.poke(context) is True
            mock_log.assert_called_once()


class TestHandleNoDbtNodeStatus:
    """Tests for BaseConsumerSensor._handle_no_dbt_node_status."""

    def _make_sensor(self):
        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
            extra_context=extra_context,
        )
        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    @patch("cosmos.operators._watcher.base.BaseConsumerSensor._fallback_to_non_watcher_run", return_value=True)
    def test_producer_failed_with_no_poke_retries_falls_back(self, mock_fallback):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 0
        context = MagicMock()

        result = sensor._handle_no_dbt_node_status("failed", try_number=1, context=context)

        assert result is True
        mock_fallback.assert_called_once_with(1, context)

    def test_producer_failed_with_poke_retries_raises(self):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 1

        with pytest.raises(AirflowException, match="dbt build command failed"):
            sensor._handle_no_dbt_node_status("failed", try_number=1, context=MagicMock())

    @patch("cosmos.operators._watcher.base.BaseConsumerSensor._fallback_to_non_watcher_run", return_value=True)
    def test_producer_skipped_falls_back(self, mock_fallback):
        sensor = self._make_sensor()
        context = MagicMock()

        result = sensor._handle_no_dbt_node_status("skipped", try_number=1, context=context)

        assert result is True
        mock_fallback.assert_called_once_with(1, context)

    def test_no_status_increments_poke_retry(self):
        sensor = self._make_sensor()
        sensor.poke_retry_number = 0

        result = sensor._handle_no_dbt_node_status(None, try_number=1, context=MagicMock())

        assert result is False
        assert sensor.poke_retry_number == 1


class TestNodeEventBuffer:
    """Tests for the in-memory per-node event buffer (local watcher log shipping)."""

    UID = "model.pkg.my_model"

    def _runner_node_finished(self, node_status="error", message="DB Error: table missing"):
        """dbt-runner mode: NodeFinished carries the error in run_result.message."""
        return {
            "info": {"name": "NodeFinished", "msg": ""},
            "data": {
                "node_info": {
                    "unique_id": self.UID,
                    "node_status": node_status,
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "run_result": {"status": node_status, "message": message},
            },
        }

    def _run_result_error(self, msg="DB Error: table missing"):
        """subprocess mode: the error arrives in a RunResultError event (node_status='None')."""
        return {
            "info": {"name": "RunResultError"},
            "data": {"node_info": {"unique_id": self.UID, "node_status": "None"}, "msg": msg},
        }

    def _flushed_value(self, buffer, **kwargs):
        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _flush_dbt_event(Mock(), buffer, self.UID, **kwargs)
        return mock_push.call_args.kwargs["value"]

    def test_runner_mode_captures_error_from_run_result_message(self):
        """dbt-runner mode: the error in NodeFinished.run_result.message reaches the consumer."""
        buffer: dict = {}
        _accumulate_dbt_log_event(buffer, self._runner_node_finished(message="DB Error: table missing"))
        value = self._flushed_value(buffer, terminal_status="error")
        assert value["status"] == "error"
        assert value["msg"] == "DB Error: table missing"
        assert value["start_time"] == "00:00:00" and value["finish_time"] == "00:01:00"
        assert "_has_error_msg" not in value  # internal flag stripped before push

    def test_subprocess_mode_captures_error_from_run_result_error_event(self):
        """subprocess mode: terminal status flushes first; the later RunResultError re-flush adds the msg."""
        buffer: dict = {}
        # Terminal status event (LogModelResult-equivalent) flushes first -- status stamped, no msg yet.
        first = self._flushed_value(buffer, terminal_status="error")
        assert first["status"] == "error" and first["msg"] is None
        # RunResultError arrives afterwards with the error text.
        err = self._run_result_error(msg="DB Error: table missing")
        _accumulate_dbt_log_event(buffer, err)
        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _flush_dbt_event_on_error(Mock(), buffer, err)
        value = mock_push.call_args.kwargs["value"]
        assert value["status"] == "error"  # not clobbered to "None"
        assert value["msg"] == "DB Error: table missing"

    def test_error_message_not_overwritten_by_later_empty_message(self):
        buffer: dict = {}
        _accumulate_dbt_log_event(buffer, self._run_result_error(msg="real error"))
        _accumulate_dbt_log_event(buffer, self._runner_node_finished(message=""))  # later, empty
        value = self._flushed_value(buffer, terminal_status="error")
        assert value["msg"] == "real error"

    def test_none_string_status_is_ignored(self):
        buffer: dict = {}
        _accumulate_dbt_log_event(buffer, self._run_result_error(msg="err"))  # node_status == "None"
        assert buffer[self.UID]["status"] is None

    @pytest.mark.parametrize("extra_kwargs", [None, {}])
    def test_store_dbt_resource_status_from_log_tolerates_falsy_extra_kwargs(self, extra_kwargs):
        """A falsy/None extra_kwargs must not raise during parsing (no context to push to)."""
        line = '{"info": {"name": "NodeFinished"}, "data": {"node_info": {"unique_id": "model.p.m", "node_status": "success"}}}'
        # Should be a no-op (no context/ti) rather than raising AttributeError.
        store_dbt_resource_status_from_log(line, extra_kwargs, node_event_buffer={})

    def test_accumulate_ignores_non_allowlisted_and_missing_unique_id(self):
        buffer: dict = {}
        _accumulate_dbt_log_event(
            buffer, {"info": {"name": "LogStartLine"}, "data": {"node_info": {"unique_id": self.UID}}}
        )
        _accumulate_dbt_log_event(buffer, {"info": {"name": "NodeFinished"}, "data": {"node_info": {}}})
        assert buffer == {}

    def test_flush_on_error_ignores_non_error_and_messageless_events(self):
        buffer: dict = {}
        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            # Not an error event -> no flush.
            _flush_dbt_event_on_error(
                Mock(), buffer, {"info": {"name": "NodeFinished"}, "data": {"node_info": {"unique_id": self.UID}}}
            )
            # Error event but no message -> no flush.
            _flush_dbt_event_on_error(
                Mock(), buffer, {"info": {"name": "RunResultError"}, "data": {"node_info": {"unique_id": self.UID}}}
            )
        mock_push.assert_not_called()

    def test_flush_on_error_when_message_only_in_info_msg(self):
        """An allowlisted error event whose text is only in info.msg must still re-flush."""
        buffer: dict = {}
        event = {
            "info": {"name": "RunResultError", "msg": "the error"},
            "data": {"node_info": {"unique_id": self.UID, "node_status": "None"}},
        }
        _accumulate_dbt_log_event(buffer, event)
        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:
            _flush_dbt_event_on_error(Mock(), buffer, event)
        assert mock_push.called
        assert mock_push.call_args.kwargs["value"]["msg"] == "the error"
