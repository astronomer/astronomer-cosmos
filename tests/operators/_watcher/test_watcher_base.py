from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowSkipException

from cosmos.operators._watcher.base import BaseConsumerSensor, _process_dbt_log_event
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
        context = Mock()
        with pytest.raises(AirflowSkipException, match="was skipped by the dbt command"):
            sensor.execute_complete(context, {"status": "skipped", "reason": "source_not_fresh"})

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
