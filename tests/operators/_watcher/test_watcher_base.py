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
