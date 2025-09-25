import base64
import json
import zlib
from types import SimpleNamespace
from unittest.mock import patch

from cosmos.config import InvocationMode
from cosmos.operators.watcher import PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT, DbtProducerWatcherOperator


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


class TestDbtModelStatusSensor:

    @pytest.fixture
    def sensor(self):
        return DbtModelStatusSensor(
            task_id="model.my_model",
            model_unique_id="model.my_model",
            project_dir="/fake/project",
            profiles_dir="/fake/profiles",
        )

    def test_filter_flags_removes_select_and_exclude(self):
        flags = ["--select", "model_a", "--exclude", "model_b", "--threads", "4"]
        expected = ["--threads", "4"]
        result = DbtModelStatusSensor._filter_flags(flags)
        assert result == expected

    def test_filter_flags_handles_no_flags(self):
        assert DbtModelStatusSensor._filter_flags([]) == []

    @patch("cosmos.operators.watcher.DbtModelStatusSensor.build_and_run_cmd")
    def test_handle_task_retry_runs_model(self, mock_build_and_run_cmd, sensor):
        mock_context = {
            "ti": Mock(),
        }

        mock_task = Mock()
        mock_task.add_cmd_flags.return_value = ["--select", "model_a", "--threads", "4"]
        mock_context["ti"].task.dag.get_task.return_value = mock_task

        result = sensor._handle_task_retry(2, mock_context)

        assert result is True
        mock_build_and_run_cmd.assert_called_once()
        called_args = mock_build_and_run_cmd.call_args[1]["cmd_flags"]
        assert "--select" in called_args
        assert "my_model" in called_args
        assert "--threads" in called_args

    def test_poke_returns_false_when_no_data(self, sensor):
        ti_mock = Mock()
        ti_mock.try_number = 1
        ti_mock.xcom_pull.return_value = None
        context = {"ti": ti_mock}
        result = sensor.poke(context)
        assert result is False

    def make_event_payload(self, status: str) -> str:
        data = {"data": {"run_result": {"status": status}}}
        compressed = zlib.compress(str(data).encode("utf-8"))
        return base64.b64encode(compressed).decode("utf-8")

    def test_poke_success_returns_true(self, sensor):
        ti_mock = Mock()
        ti_mock.try_number = 1
        ti_mock.xcom_pull.side_effect = [
            [],  # dbt_startup_events
            [],  # pipeline_outlets
            self.make_event_payload("success"),  # node_finished_key
        ]
        context = {"ti": ti_mock}
        assert sensor.poke(context) is True

    def test_poke_failure_raises_exception(self, sensor):
        ti_mock = Mock()
        ti_mock.try_number = 1
        ti_mock.xcom_pull.side_effect = [
            None,  # dbt_startup_events
            None,  # pipeline_outlets
            self.make_event_payload("error"),  # node_finished_key
        ]
        context = {"ti": ti_mock}
        with pytest.raises(AirflowException, match="finished with status 'error'"):
            sensor.poke(context)

    @patch.object(DbtModelStatusSensor, "_handle_task_retry")
    def test_poke_calls_retry_on_retry_attempt(self, mock_retry, sensor):
        ti_mock = Mock()
        ti_mock.try_number = 2
        context = {"ti": ti_mock}
        sensor.poke(context)
        mock_retry.assert_called_once_with(2, context)
