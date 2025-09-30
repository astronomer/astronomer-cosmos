import base64
import json
import zlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from cosmos.config import InvocationMode
from cosmos.operators.watcher import (
    PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT,
    DbtConsumerWatcherSensor,
    DbtProducerWatcherOperator,
)


class _MockTI:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def xcom_push(self, key: str, value: str, **_):
        self.store[key] = value


class _MockContext(dict):
    pass


def _fake_event(name: str = "NodeFinished", uid: str = "model.pkg.m"):
    """Create a minimal fake EventMsg-like object suitable for helper tests."""

    class _Info(SimpleNamespace):
        pass

    class _NodeInfo(SimpleNamespace):
        pass

    class _RunResult(SimpleNamespace):
        pass

    node_info = _NodeInfo(unique_id=uid)
    run_result = _RunResult(status="success", message="ok")

    data = SimpleNamespace(node_info=node_info, run_result=run_result)
    info = _Info(name=name, code="X", msg="msg")
    return SimpleNamespace(info=info, data=data)


@patch("google.protobuf.json_format.MessageToDict")
def test_serialize_event(mock_mtd):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)

    mock_mtd.side_effect = lambda ev, **kwargs: {"dummy": True}

    out = op._serialize_event(_fake_event())
    assert out == {"dummy": True}
    mock_mtd.assert_called()


def test_dbt_producer_watcher_operator_priority_weight_default():
    """Test that DbtProducerWatcherOperator uses default priority_weight of 9999."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.priority_weight == PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT


def test_dbt_producer_watcher_operator_priority_weight_override():
    """Test that DbtProducerWatcherOperator allows overriding priority_weight."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, priority_weight=100)
    assert op.priority_weight == 100


def test_handle_startup_event():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    lst: list[dict] = []
    ev = _fake_event("MainReportVersion")
    op._handle_startup_event(ev, lst)
    assert lst and lst[0]["name"] == "MainReportVersion"


def test_handle_node_finished_pushes_xcom():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    with patch.object(op, "_serialize_event", return_value={"foo": "bar"}):
        ev = _fake_event()
        op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = zlib.decompress(base64.b64decode(stored)).decode()
    assert json.loads(raw) == {"foo": "bar"}


def test_execute_streaming_mode():
    """Streaming path should push startup + per-model XComs."""
    from contextlib import nullcontext

    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    op.invocation_mode = InvocationMode.DBT_RUNNER

    import cosmos.operators.watcher as _watch_mod

    # Ensure EventMsg symbol exists without permanently altering the module
    if _watch_mod.EventMsg is None:

        class _DummyEv:
            pass

        eventmsg_patch = patch("cosmos.operators.watcher.EventMsg", _DummyEv, create=True)
    else:
        eventmsg_patch = nullcontext()  # type: ignore

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "dummy"}

    main_rep = _fake_event("MainReportVersion")
    node_evt = _fake_event("NodeFinished", uid="model.pkg.x")

    def fake_base_execute(self, context=None, **_):  # type: ignore[override]
        for cb in getattr(self, "_dbt_runner_callbacks", []):
            cb(main_rep)
            cb(node_evt)
        return None

    with eventmsg_patch, patch.object(
        DbtProducerWatcherOperator,
        "_serialize_event",
        lambda self, ev: {"dummy": True},
    ), patch(
        "cosmos.operators.watcher.DbtLocalBaseOperator.execute",
        fake_base_execute,
    ):
        op.execute(context=ctx)

    assert "dbt_startup_events" in ti.store

    node_key = "nodefinished_model__pkg__x"
    assert node_key in ti.store


def test_execute_fallback_mode(tmp_path):
    """Fallback path pushes compressed run_results once."""

    tgt = tmp_path / "target"
    tgt.mkdir()
    with (tgt / "run_results.json").open("w") as fp:
        json.dump({"results": [{"unique_id": "a", "status": "success"}]}, fp)

    op = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    op.invocation_mode = InvocationMode.SUBPROCESS  # force fallback

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "x"}

    def fake_build_run(self, context, **kw):
        from cosmos.operators.local import AbstractDbtLocalBase

        AbstractDbtLocalBase._handle_post_execution(self, self.project_dir, context, True)
        return None

    with patch("cosmos.operators.local.DbtLocalBaseOperator.build_and_run_cmd", fake_build_run):
        op.execute(context=ctx)

    compressed = ti.store.get("run_results")
    assert compressed
    data = json.loads(zlib.decompress(base64.b64decode(compressed)).decode())
    assert data["results"][0]["status"] == "success"


@patch("cosmos.dbt.runner.is_available", return_value=False)
@patch("cosmos.operators.watcher.DbtLocalBaseOperator.execute", return_value="done")
def test_execute_discovers_invocation_mode(_mock_execute, _mock_is_available):
    """If invocation_mode is unset, execute() should discover and set it."""

    from cosmos.config import InvocationMode

    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.invocation_mode is None  # precondition

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "xyz"}

    result = op.execute(context=ctx)

    assert result == "done"
    assert op.invocation_mode == InvocationMode.SUBPROCESS


MODEL_UNIQUE_ID = "model.jaffle_shop.stg_orders"
ENCODED_RUN_RESULTS = base64.b64encode(
    zlib.compress(b'{"results":[{"unique_id":"model.jaffle_shop.stg_orders","status":"success"}]}')
).decode("utf-8")

ENCODED_RUN_RESULTS_FAILED = base64.b64encode(
    zlib.compress(b'{"results":[{"unique_id":"model.jaffle_shop.stg_orders","status":"fail"}]}')
).decode("utf-8")

ENCODED_EVENT = base64.b64encode(zlib.compress(b'{"data": {"run_result": {"status": "success"}}}')).decode("utf-8")


class TestDbtConsumerWatcherSensor:

    def make_sensor(self, **kwargs):
        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        kwargs["extra_context"] = extra_context
        sensor = DbtConsumerWatcherSensor(
            task_id="model.my_model",
            project_dir="/tmp/project",
            profile_config=None,
            **kwargs,
        )

        sensor.invocation_mode = "DBT_RUNNER"
        return sensor

    def make_context(self, ti_mock):
        return {"ti": ti_mock}

    @patch("cosmos.operators.watcher.EventMsg")
    def test_poke_status_none_from_events(self, MockEventMsg):
        mock_event_instance = MagicMock()
        mock_event_instance.status = "done"
        MockEventMsg.return_value = mock_event_instance

        sensor = self.make_sensor()
        sensor.invocation_mode = InvocationMode.DBT_RUNNER
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.side_effect = [None, None]  # no event msg found
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is False

    def test_poke_success_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "SUBPROCESS"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    def test_invocation_mode_none(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = None

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    def test_poke_failure_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "OTHER_MODE"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS_FAILED
        context = self.make_context(ti)

        with pytest.raises(AirflowException):
            sensor.poke(context)

    def test_poke_status_none_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "OTHER_MODE"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is False

    @patch("cosmos.operators.local.AbstractDbtLocalBase.build_and_run_cmd")
    def test_task_retry(self, mock_build_and_run_cmd):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 2
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        sensor.poke(context)
        mock_build_and_run_cmd.assert_called_once()

    def test_handle_task_retry(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--select", "some_model", "--threads", "2"]
        context = self.make_context(ti)
        sensor.build_and_run_cmd = MagicMock()

        result = sensor._handle_task_retry(2, context)

        assert result is True
        sensor.build_and_run_cmd.assert_called_once()
        args, kwargs = sensor.build_and_run_cmd.call_args
        assert "--select" in kwargs["cmd_flags"]
        assert MODEL_UNIQUE_ID.split(".")[-1] in kwargs["cmd_flags"]

    def test_filter_flags(self):
        flags = ["--select", "model", "--exclude", "other", "--threads", "2"]
        expected = ["--threads", "2"]

        result = DbtConsumerWatcherSensor._filter_flags(flags)

        assert result == expected

    def test_get_status_from_run_results_success(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS

        result = sensor._get_status_from_run_results(ti)
        assert result == "success"

    def test_get_status_from_run_results_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = None

        result = sensor._get_status_from_run_results(ti)
        assert result is None

    def test_get_status_from_events_success(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, ENCODED_EVENT]

        result = sensor._get_status_from_events(ti)
        assert result == "success"

    def test_get_status_from_events_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, None]

        result = sensor._get_status_from_events(ti)
        assert result is None
