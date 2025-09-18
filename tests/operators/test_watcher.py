import base64
import gzip
import json
from types import SimpleNamespace

from cosmos.config import InvocationMode
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


def test_serialize_event(monkeypatch):
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)

    called = {}

    def _fake_msgtodict(ev, **kwargs):
        called["done"] = True
        return {"dummy": True}

    monkeypatch.setattr("google.protobuf.json_format.MessageToDict", _fake_msgtodict)
    out = op._serialize_event(_fake_event())
    assert out == {"dummy": True}
    assert called["done"]


def test_handle_startup_event():
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    lst: list[dict] = []
    ev = _fake_event("MainReportVersion")
    op._handle_startup_event(ev, lst)
    assert lst and lst[0]["name"] == "MainReportVersion"


def test_handle_node_finished_pushes_xcom(monkeypatch):
    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    monkeypatch.setattr(op, "_serialize_event", lambda ev: {"foo": "bar"})

    ev = _fake_event()
    op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = gzip.decompress(base64.b64decode(stored)).decode()
    assert json.loads(raw) == {"foo": "bar"}


def test_execute_streaming_mode(monkeypatch):
    """Streaming path should push startup + per-model XComs."""

    op = DbtBuildCoordinatorOperator(project_dir=".", profile_config=None)
    op.invocation_mode = InvocationMode.DBT_RUNNER

    import cosmos.operators.watcher as _watch_mod

    if _watch_mod.EventMsg is None:

        class _DummyEv:
            pass

        _watch_mod.EventMsg = _DummyEv

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "dummy"}

    main_rep = _fake_event("MainReportVersion")
    node_evt = _fake_event("NodeFinished", uid="model.pkg.x")

    def fake_patch(self, cb):
        cb(main_rep)
        cb(node_evt)
        from contextlib import nullcontext

        return nullcontext()

    monkeypatch.setattr(DbtBuildCoordinatorOperator, "_patch_runner", fake_patch)

    monkeypatch.setattr(DbtBuildCoordinatorOperator, "_serialize_event", lambda self, ev: {"dummy": True})

    monkeypatch.setattr("cosmos.operators.watcher.DbtBuildLocalOperator.execute", lambda *_, **__: None)

    op.execute(context=ctx)

    assert "dbt_startup_events" in ti.store

    node_key = "nodefinished_model__pkg__x"
    assert node_key in ti.store


def test_execute_fallback_mode(monkeypatch, tmp_path):
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

    monkeypatch.setattr("cosmos.operators.local.DbtBuildLocalOperator.build_and_run_cmd", fake_build_run)

    op.execute(context=ctx)

    compressed = ti.store.get("run_results")
    assert compressed
    data = json.loads(gzip.decompress(base64.b64decode(compressed)).decode())
    assert data["results"][0]["status"] == "success"
