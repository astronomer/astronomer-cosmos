from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from cosmos.operators._watcher.triggerer import WatcherTrigger

_real_import = __import__


@pytest.mark.asyncio
class TestWatcherTrigger:

    def setup_method(self):
        self.trigger = WatcherTrigger(
            model_unique_id="model.test",
            producer_task_id="task_1",
            dag_id="dag_1",
            run_id="run_123",
            map_index=None,
            use_event=True,
            poke_interval=0.001,  # fast polling
        )

    def test_serialize(self):
        classpath, args = self.trigger.serialize()
        assert classpath.endswith("WatcherTrigger")
        assert args["model_unique_id"] == "model.test"
        assert args["poke_interval"] == 0.001

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Require Airflow < 3.0.0")
    @pytest.mark.asyncio
    async def test_get_xcom_val_af3(self):
        expected_value = {"foo": "bar"}

        with patch("cosmos.operators._watcher.triggerer.sync_to_async") as mock_sync_to_async:
            mock_get_one = AsyncMock(return_value=expected_value)
            mock_sync_to_async.return_value = mock_get_one

            result = await self.trigger.get_xcom_val_af3("test_key")

            mock_sync_to_async.assert_called_once()
            mock_get_one.assert_awaited_once_with(
                run_id="run_123",
                key="test_key",
                task_id="task_1",
                dag_id="dag_1",
                map_index=None,
            )
            assert result == expected_value

    @pytest.mark.skipif(AIRFLOW_VERSION >= Version("3.0.0"), reason="Require Airflow < 3.0.0")
    @pytest.mark.asyncio
    async def test_get_xcom_val_af2(self):
        expected_xcom = {"bar": "baz"}

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = expected_xcom

        mock_session = MagicMock()
        mock_query = mock_session.__enter__.return_value.query.return_value.filter_by.return_value
        mock_query.one_or_none.side_effect = [mock_ti, None]

        with patch("airflow.utils.session.create_session", return_value=mock_session):

            def wrap_sync(func):
                async def runner(*args, **kwargs):
                    return func(*args, **kwargs)

                return runner

            with patch("cosmos.operators._watcher.triggerer.sync_to_async", side_effect=wrap_sync):
                result = await self.trigger.get_xcom_val_af2("test_key")

                mock_ti.xcom_pull.assert_called_once_with(task_ids="task_1", key="test_key")
                assert result == expected_xcom

                # Missing TaskInstance should result in None without error
                none_result = await self.trigger.get_xcom_val_af2("test_key")
                assert none_result is None

    @pytest.mark.parametrize(
        "use_event, xcom_val, expected_status, expected_compiled_sql",
        [
            (True, {"data": {"run_result": {"status": "success"}}, "compiled_sql": "SELECT 1"}, "success", "SELECT 1"),
            (True, {"data": {"run_result": {"status": "success"}}}, "success", None),
            (True, None, None, None),
            (False, "failed", "failed", None),
            (False, "success", "success", "SELECT * FROM table"),
        ],
    )
    async def test_parse_node_status_and_compiled_sql(
        self, use_event, xcom_val, expected_status, expected_compiled_sql
    ):
        self.trigger.use_event = use_event

        async def mock_get_xcom_val(key):
            if use_event:
                return xcom_val if xcom_val else None
            else:
                # For subprocess mode, simulate status and compiled_sql keys
                if key.endswith("_status"):
                    return xcom_val
                elif key.endswith("_compiled_sql"):
                    return expected_compiled_sql
            return None

        with (
            patch("cosmos.operators._watcher.triggerer._parse_compressed_xcom", return_value=xcom_val),
            patch.object(self.trigger, "get_xcom_val", AsyncMock(side_effect=mock_get_xcom_val)),
        ):
            status, compiled_sql = await self.trigger._parse_node_status_and_compiled_sql()
            assert status == expected_status
            assert compiled_sql == expected_compiled_sql

    @pytest.mark.parametrize(
        "airflow_version, expected_val",
        [
            (Version("2.11.0"), "af2"),  # Airflow < 3 uses get_xcom_val_af2
            (Version("3.0.0"), "af3"),  # Airflow >= 3 uses get_xcom_val_af3
        ],
    )
    async def test_get_xcom_val_branches(self, airflow_version, expected_val):
        with patch("cosmos.operators._watcher.triggerer.AIRFLOW_VERSION", airflow_version):
            if expected_val == "af2":
                with patch.object(self.trigger, "get_xcom_val_af2", AsyncMock(return_value="af2")):
                    val = await self.trigger.get_xcom_val("key")
                    assert val == "af2"
            else:
                with patch.object(self.trigger, "get_xcom_val_af3", AsyncMock(return_value="af3")):
                    val = await self.trigger.get_xcom_val("key")
                    assert val == "af3"

    @pytest.mark.parametrize(
        "node_status, producer_state, expected",
        [
            ("success", "running", {"status": "success"}),
            ("failed", "running", {"status": "failed", "reason": "model_failed"}),
            (None, "failed", {"status": "failed", "reason": "producer_failed"}),
            (None, "success", {"status": "success", "reason": "model_not_run"}),
        ],
    )
    async def test_run_various_outcomes(self, node_status, producer_state, expected):

        async def fake_get_xcom_val(key):
            return "compressed_data"

        with (
            patch.object(self.trigger, "get_xcom_val", side_effect=fake_get_xcom_val),
            patch.object(self.trigger, "_get_producer_task_status", AsyncMock(return_value=producer_state)),
            patch(
                "cosmos.operators._watcher.triggerer._parse_compressed_xcom",
                return_value={"data": {"run_result": {"status": node_status}}} if node_status else {},
            ),
        ):
            events = [event async for event in self.trigger.run()]
            assert events[0].payload == expected

    @pytest.mark.asyncio
    async def test_get_producer_task_status_airflow2(self):
        fetcher = MagicMock(return_value="failed")
        with patch("cosmos.operators._watcher.triggerer.AIRFLOW_VERSION", Version("2.9.0")):
            with patch(
                "cosmos.operators._watcher.triggerer.build_producer_state_fetcher", return_value=fetcher
            ) as mock_builder:
                state = await self.trigger._get_producer_task_status()

        mock_builder.assert_called_once_with(
            airflow_version=Version("2.9.0"),
            dag_id=self.trigger.dag_id,
            run_id=self.trigger.run_id,
            producer_task_id=self.trigger.producer_task_id,
            logger=ANY,
        )
        fetcher.assert_called_once_with()
        assert state == "failed"

    @pytest.mark.asyncio
    async def test_get_producer_task_status_airflow2_missing_ti(self):
        fetcher = MagicMock(return_value=None)

        with patch("cosmos.operators._watcher.triggerer.AIRFLOW_VERSION", Version("2.9.0")):
            with patch("cosmos.operators._watcher.triggerer.build_producer_state_fetcher", return_value=fetcher):
                state = await self.trigger._get_producer_task_status()

        fetcher.assert_called_once_with()
        assert state is None

    @pytest.mark.asyncio
    async def test_get_producer_task_status_airflow3(self):
        fetcher = MagicMock(return_value="success")

        with patch("cosmos.operators._watcher.triggerer.AIRFLOW_VERSION", Version("3.0.0")):
            with patch("cosmos.operators._watcher.triggerer.build_producer_state_fetcher", return_value=fetcher):
                state = await self.trigger._get_producer_task_status()

        fetcher.assert_called_once_with()
        assert state == "success"

    @pytest.mark.asyncio
    async def test_get_producer_task_status_airflow3_missing_state(self):
        fetcher = MagicMock(return_value=None)
        with patch("cosmos.operators._watcher.triggerer.AIRFLOW_VERSION", Version("3.0.0")):
            with patch("cosmos.operators._watcher.triggerer.build_producer_state_fetcher", return_value=fetcher):
                state = await self.trigger._get_producer_task_status()

        fetcher.assert_called_once_with()
        assert state is None

    @pytest.mark.skipif(
        AIRFLOW_VERSION < Version("3.0.0"), reason="RuntimeTaskInstance import exists on Airflow >= 3.0"
    )
    @pytest.mark.asyncio
    async def test_get_producer_task_status_airflow3_import_error(self):
        def _import_side_effect(name: str, *args, **kwargs):
            if name == "airflow.sdk.execution_time.task_runner":
                raise ModuleNotFoundError("missing runtime")
            return _real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_import_side_effect):
            state = await self.trigger._get_producer_task_status()

        assert state is None

    @pytest.mark.asyncio
    async def test_run_producer_success_model_not_run(self, caplog):
        """Test that when producer succeeds but model has no status, trigger yields success with model_not_run reason."""
        get_producer_status_mock = AsyncMock(return_value="success")
        parse_node_status_and_compiled_sql_mock = AsyncMock(return_value=(None, None))

        caplog.set_level("INFO")

        with (
            patch.object(self.trigger, "_get_producer_task_status", get_producer_status_mock),
            patch.object(self.trigger, "_parse_node_status_and_compiled_sql", parse_node_status_and_compiled_sql_mock),
        ):
            events = []
            async for event in self.trigger.run():
                events.append(event)

        assert len(events) == 1
        assert events[0].payload == {"status": "success", "reason": "model_not_run"}
        assert "The producer task 'task_1' succeeded" in caplog.text
        assert "There is no information about the model 'model.test' execution" in caplog.text

    @pytest.mark.asyncio
    async def test_run_poke_interval_and_debug_log(self, caplog):
        get_xcom_val_mock = AsyncMock(side_effect=["compressed_data"])
        get_producer_status_mock = AsyncMock(side_effect=["running", "running", "running"])
        parse_node_status_and_compiled_sql_mock = AsyncMock(
            side_effect=[(None, None), (None, None), ("success", "SELECT 1")]
        )

        caplog.set_level("DEBUG")

        with (
            patch.object(self.trigger, "get_xcom_val", get_xcom_val_mock),
            patch.object(self.trigger, "_get_producer_task_status", get_producer_status_mock),
            patch.object(self.trigger, "_parse_node_status_and_compiled_sql", parse_node_status_and_compiled_sql_mock),
            patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
        ):
            events = []
            async for event in self.trigger.run():
                events.append(event)

            sleep_mock.assert_awaited()

        assert events[0].payload["status"] == "success"
        assert events[0].payload["compiled_sql"] == "SELECT 1"
