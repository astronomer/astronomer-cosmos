from unittest.mock import AsyncMock, patch

import pytest
from packaging.version import Version

from cosmos._triggers.watcher import WatcherTrigger


@pytest.mark.asyncio
class TestWatcherTrigger:

    def setup_method(self):
        self.trigger = WatcherTrigger(
            model_unique_id="model.test",
            producer_task_id="task_1",
            dag_id="dag_1",
            run_id="run_123",
            map_index=0,
            use_event=True,
            poke_interval=0.001,  # fast polling
        )

    @pytest.mark.parametrize(
        "use_event, xcom_val, expected_status",
        [
            (True, {"data": {"run_result": {"status": "success"}}}, "success"),
            (False, {"results": [{"unique_id": "model.test", "status": "failed"}]}, "failed"),
        ],
    )
    async def test_parse_node_status(self, use_event, xcom_val, expected_status):
        self.trigger.use_event = use_event
        with patch("cosmos._triggers.watcher._parse_compressed_xcom", return_value=xcom_val), patch.object(
            self.trigger, "get_xcom_val", AsyncMock(return_value="compressed_data")
        ):
            status = await self.trigger._parse_node_status()
            assert status == expected_status

    @pytest.mark.parametrize(
        "airflow_version, expected_val",
        [
            (Version("2.5.0"), "af2"),  # Airflow < 3 uses get_xcom_val_af2
            (Version("3.0.0"), "af3"),  # Airflow >= 3 uses get_xcom_val_af3
        ],
    )
    async def test_get_xcom_val_branches(self, airflow_version, expected_val):
        with patch("cosmos._triggers.watcher.AIRFLOW_VERSION", airflow_version):
            if expected_val == "af2":
                with patch.object(self.trigger, "get_xcom_val_af2", AsyncMock(return_value="af2")):
                    val = await self.trigger.get_xcom_val("key")
                    assert val == "af2"
            else:
                with patch.object(self.trigger, "get_xcom_val_af3", AsyncMock(return_value="af3")):
                    val = await self.trigger.get_xcom_val("key")
                    assert val == "af3"

    @pytest.mark.parametrize(
        "node_status, dr_state, expected",
        [
            ("success", "running", "success"),
            ("failed", "running", "failed"),
            (None, "failed", "failed"),
        ],
    )
    async def test_run_various_outcomes(self, node_status, dr_state, expected):

        async def fake_get_xcom_val(key):
            return dr_state if key == "state" else "compressed_data"

        with patch.object(self.trigger, "get_xcom_val", side_effect=fake_get_xcom_val), patch(
            "cosmos._triggers.watcher._parse_compressed_xcom",
            return_value={"data": {"run_result": {"status": node_status}}} if node_status else {},
        ):
            events = [event async for event in self.trigger.run()]
            assert events[0].payload["status"] == expected
