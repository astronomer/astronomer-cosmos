import base64
import gzip
import json
from types import SimpleNamespace
from unittest.mock import patch

from cosmos.config import InvocationMode, RenderConfig
from cosmos.operators.watcher import DbtBuildCoordinatorOperator


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
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)

    mock_mtd.side_effect = lambda ev, **kwargs: {"dummy": True}

    out = op._serialize_event(_fake_event())
    assert out == {"dummy": True}
    mock_mtd.assert_called()


def test_handle_startup_event():
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    lst: list[dict] = []
    ev = _fake_event("MainReportVersion")
    op._handle_startup_event(ev, lst)
    assert lst and lst[0]["name"] == "MainReportVersion"


def test_handle_node_finished_pushes_xcom():
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    with patch.object(op, "_serialize_event", return_value={"foo": "bar"}):
        ev = _fake_event()
        op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = gzip.decompress(base64.b64decode(stored)).decode()
    assert json.loads(raw) == {"foo": "bar"}


def test_execute_streaming_mode():
    """Streaming path should push startup + per-model XComs."""
    from contextlib import nullcontext

    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
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

    def fake_patch(self, cb):
        cb(main_rep)
        cb(node_evt)

        return nullcontext()

    with eventmsg_patch, patch.object(DbtBuildCoordinatorOperator, "_patch_runner", fake_patch), patch.object(
        DbtBuildCoordinatorOperator, "_serialize_event", lambda self, ev: {"dummy": True}
    ), patch("cosmos.operators.watcher.DbtBuildLocalOperator.execute", lambda *_, **__: None):
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

    op = DbtBuildCoordinatorOperator(project_dir=str(tmp_path), profile_config=None)
    op.invocation_mode = InvocationMode.SUBPROCESS  # force fallback

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "x"}

    def fake_build_run(self, context, **kw):
        from cosmos.operators.local import AbstractDbtLocalBase

        AbstractDbtLocalBase._handle_post_execution(self, self.project_dir, context, True)
        return None

    with patch("cosmos.operators.local.DbtBuildLocalOperator.build_and_run_cmd", fake_build_run):
        op.execute(context=ctx)

    compressed = ti.store.get("run_results")
    assert compressed
    data = json.loads(gzip.decompress(base64.b64decode(compressed)).decode())
    assert data["results"][0]["status"] == "success"


@patch(
    "cosmos.operators.watcher.DbtBuildLocalOperator.add_cmd_flags",
    return_value=["cmd"],
)
def test_add_cmd_flags_includes_select_and_exclude(_mock_add):
    """add_cmd_flags should append --exclude/--select from RenderConfig."""

    rc = RenderConfig(select=["tag:nightly"], exclude=["model.old"])
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None, render_config=rc)

    flags = op.add_cmd_flags()

    assert flags[0] == "cmd"
    assert "--exclude" in flags and "model.old" in flags
    assert "--select" in flags and "tag:nightly" in flags


def test_patch_runner_patches_and_restores():
    """_patch_runner should temporarily replace cosmos.dbt.runner.get_runner."""
    import cosmos.dbt.runner as dr

    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)

    original = dr.get_runner
    called = {}

    def dummy_callback(ev):
        called["cb"] = True

    class _FakeRunner:
        def __init__(self, callbacks=None):
            self.callbacks = callbacks or []

    import sys
    import types

    fake_main = types.ModuleType("dbt.cli.main")
    fake_main.dbtRunner = _FakeRunner
    with patch.dict(
        sys.modules, {"dbt": types.ModuleType("dbt"), "dbt.cli": types.ModuleType("dbt.cli"), "dbt.cli.main": fake_main}
    ):
        with op._patch_runner(dummy_callback):
            runner_instance = dr.get_runner()
            assert isinstance(runner_instance, _FakeRunner)
            assert dummy_callback in runner_instance.callbacks

    assert dr.get_runner is original


@patch("cosmos.dbt.runner.is_available", return_value=False)
@patch("cosmos.operators.watcher.DbtBuildLocalOperator.execute", return_value="done")
def test_execute_discovers_invocation_mode(_mock_execute, _mock_is_available):
    """If invocation_mode is unset, execute() should discover and set it."""

    from cosmos.config import InvocationMode

    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    assert op.invocation_mode is None  # precondition

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "xyz"}

    result = op.execute(context=ctx)

    assert result == "done"
    assert op.invocation_mode == InvocationMode.SUBPROCESS
