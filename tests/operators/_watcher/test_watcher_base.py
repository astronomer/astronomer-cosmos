from unittest.mock import Mock, patch

import pytest

from cosmos.operators._watcher.base import BaseConsumerSensor, _process_dbt_log_event
from cosmos.operators.local import DbtRunLocalOperator


class TestBaseConsumerSensor:

    def test__methods_to_be_implemented(self):
        class SubclassBaseConsumerSensor(BaseConsumerSensor, DbtRunLocalOperator):
            something_to_be_implemented = True

        sensor = SubclassBaseConsumerSensor(
            task_id="test_sensor",
            model_unique_id="model.jaffle_shop.stg_orders",
            producer_task_id="dbt_run_local",
            profile_config=None,
            project_dir="/tmp/sample_project",
        )
        with pytest.raises(NotImplementedError):
            sensor.use_event()

        with pytest.raises(NotImplementedError):
            assert sensor._get_status_from_events(None, None) is None

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
        "msg,should_push",
        [
            ("1 of 30 START sql table model bq_dev.a ......................... [RUN]", False),
            ("6 of 30 FAIL creating sql view model bq_dev.stg_customers ............... [ERROR in 1.18s]", True),
            ("6 of 30 ERROR creating sql view model bq_dev.stg_customers ..................... [ERROR in 1.18s]", True),
        ],
    )
    def test_process_dbt_log_event_sensitive_words(self, msg, should_push):
        task_instance = Mock()

        dbt_log = {
            "data": {
                "node_info": {
                    "unique_id": "model.test.my_model",
                    "node_status": "None",
                    "node_started_at": "2024-01-01T00:00:00",
                    "node_finished_at": "2024-01-01T00:01:00",
                },
                "msg": msg,
            },
            "info": {},
        }

        with patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push:

            _process_dbt_log_event(task_instance, dbt_log)

            if should_push:
                mock_push.assert_called_once()
            else:
                mock_push.assert_not_called()

    def test_process_dbt_log_event_skips_duplicate_event(self):
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
            "info": {},
        }

        duplicate_event = {
            "status": "success",
            "start_time": "2024-01-01T00:00:00",
            "finish_time": "2024-01-01T00:01:00",
            "msg": "model finished",
        }

        with (
            patch(
                "cosmos.operators._watcher.base.get_xcom_val",
                return_value=duplicate_event,
            ),
            patch("cosmos.operators._watcher.base.safe_xcom_push") as mock_push,
            patch(
                "cosmos.operators._watcher.base._iso_to_string",
                side_effect=lambda x: x,
            ),
        ):
            result = _process_dbt_log_event(task_instance, dbt_log)

            assert result is None
            mock_push.assert_not_called()
