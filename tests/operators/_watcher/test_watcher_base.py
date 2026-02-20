import pytest

from cosmos.operators._watcher.base import BaseConsumerSensor
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
