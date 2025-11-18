from __future__ import annotations

import base64
import json
import logging
import zlib
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.state import DagRunState
from packaging.version import Version

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, TestBehavior
from cosmos._triggers.watcher import WatcherTrigger
from cosmos.config import InvocationMode
from cosmos.constants import ExecutionMode
from cosmos.operators.watcher import (
    PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT,
    DbtBuildWatcherOperator,
    DbtConsumerWatcherSensor,
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtTestWatcherOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping, get_automatic_profile_mapping
from tests.utils import AIRFLOW_VERSION, new_test_dag

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"


project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


class _MockTI:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.try_number = 1

    def xcom_push(self, key: str, value: str, **_):
        self.store[key] = value


class _MockContext(dict):
    pass


def _fake_event(
    name: str = "NodeFinished", uid: str = "model.pkg.m", resource_type: str | None = None, node_path: str | None = None
):
    """Create a minimal fake EventMsg-like object suitable for helper tests."""

    class _Info(SimpleNamespace):
        pass

    class _NodeInfo(SimpleNamespace):
        pass

    class _RunResult(SimpleNamespace):
        pass

    node_info = _NodeInfo(unique_id=uid)
    if resource_type is not None:
        setattr(node_info, "resource_type", resource_type)
    if node_path is not None:
        setattr(node_info, "node_path", node_path)
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


def test_dbt_producer_watcher_operator_retries_forced_to_zero():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.retries == 0


def test_dbt_producer_watcher_operator_retries_ignores_user_input():
    user_default_args = {"retries": 5}
    op = DbtProducerWatcherOperator(
        project_dir=".",
        profile_config=None,
        default_args=user_default_args,
        retries=3,
    )

    assert op.retries == 0
    assert user_default_args["retries"] == 5


def test_dbt_producer_watcher_operator_pushes_completion_status():
    """Test that operator pushes 'completed' status to XCom in both success and failure cases."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    mock_ti = _MockTI()
    context = {"ti": mock_ti}

    # Test success case
    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        op.execute(context=context)

        # Verify status was pushed
        assert mock_ti.store.get("task_status") == "completed"
        # Verify parent execute was called
        mock_execute.assert_called_once()

    # Reset mock and store
    mock_ti.store.clear()

    # Test failure case
    class TestException(Exception):
        pass

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        mock_execute.side_effect = TestException("test error")

        with pytest.raises(TestException):
            op.execute(context=context)

        # Verify completed status was pushed even in failure case
        assert mock_ti.store.get("task_status") == "completed"
        # Verify parent execute was called
        mock_execute.assert_called_once()


def test_dbt_producer_watcher_operator_requires_task_instance():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    context: dict[str, object] = {}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        with pytest.raises(AirflowException) as excinfo:
            op.execute(context=context)

    mock_execute.assert_not_called()
    assert "expects a task instance" in str(excinfo.value)


def test_handle_startup_event():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    lst: list[dict] = []
    ev = _fake_event("MainReportVersion")
    op._handle_startup_event(ev, lst)
    assert lst and lst[0]["name"] == "MainReportVersion"


def test_dbt_producer_watcher_operator_logs_retry_message(caplog):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ti.try_number = 1
    context = {"ti": ti}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute", return_value="ok") as mock_execute:
        with caplog.at_level(logging.INFO):
            op.execute(context=context)

    mock_execute.assert_called_once()
    assert any("forces Airflow retries to 0" in message for message in caplog.messages)


def test_dbt_producer_watcher_operator_blocks_retry_attempt(caplog):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ti.try_number = 2
    context = {"ti": ti}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        with caplog.at_level(logging.ERROR):
            with pytest.raises(AirflowException) as excinfo:
                op.execute(context=context)

    mock_execute.assert_not_called()
    assert "does not support Airflow retries" in str(excinfo.value)
    assert any("does not support Airflow retries" in message for message in caplog.messages)


@pytest.mark.parametrize(
    "event, expected_message",
    [
        ({"status": "success"}, None),
        (
            {"status": "failed", "reason": "model_failed"},
            "dbt model 'model.pkg.m' failed. Review the producer task 'dbt_producer_watcher_operator' logs for details.",
        ),
        (
            {"status": "failed", "reason": "producer_failed"},
            "Watcher producer task 'dbt_producer_watcher_operator' failed before reporting model results. Check its logs for the underlying error.",
        ),
    ],
)
def test_dbt_consumer_watcher_sensor_execute_complete(event, expected_message):
    sensor = DbtConsumerWatcherSensor(
        project_dir=".",
        profiles_dir=".",
        profile_config=profile_config,
        model_unique_id="model.pkg.m",
        poke_interval=1,
        producer_task_id="dbt_producer_watcher_operator",
        task_id="consumer_sensor",
    )
    sensor.model_unique_id = "model.pkg.m"

    context = {"dag_run": MagicMock()}

    if expected_message is None:
        sensor.execute_complete(context, event)
        return

    with pytest.raises(AirflowException) as excinfo:
        sensor.execute_complete(context, event)

    assert str(excinfo.value) == expected_message


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


def test_handle_node_finished_injects_compiled_sql(tmp_path, monkeypatch):
    op = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    # Create compiled SQL file at expected path: target/compiled/pkg/models/my_model.sql
    compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
    compiled_dir.mkdir(parents=True)
    compiled_file = compiled_dir / "my_model.sql"
    sql_text = "select 1"
    compiled_file.write_text(sql_text, encoding="utf-8")

    # Ensure watcher looks up under this tmp project dir
    monkeypatch.chdir(tmp_path)

    with patch.object(op, "_serialize_event", return_value={}):
        ev = _fake_event(name="NodeFinished", uid="model.pkg.my_model", resource_type="model", node_path="my_model.sql")
        op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = zlib.decompress(base64.b64decode(stored)).decode()
    data = json.loads(raw)
    assert data.get("compiled_sql") == sql_text


def test_handle_node_finished_without_compiled_sql_does_not_inject(tmp_path, monkeypatch):
    op = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    # Ensure watcher looks up under this tmp project dir, but do NOT create compiled file
    monkeypatch.chdir(tmp_path)

    with patch.object(op, "_serialize_event", return_value={}):
        ev = _fake_event(name="NodeFinished", uid="model.pkg.my_model", resource_type="model", node_path="my_model.sql")
        op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = zlib.decompress(base64.b64decode(stored)).decode()
    data = json.loads(raw)
    assert "compiled_sql" not in data


def test_execute_streaming_mode():
    """Streaming path should push startup + per-model XComs."""
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

    with (
        eventmsg_patch,
        patch.object(
            DbtProducerWatcherOperator,
            "_serialize_event",
            lambda self, ev: {"dummy": True},
        ),
        patch(
            "cosmos.operators.watcher.DbtLocalBaseOperator.execute",
            fake_base_execute,
        ),
    ):
        op.execute(context=ctx)

    assert "dbt_startup_events" in ti.store

    node_key = "nodefinished_model__pkg__x"
    assert node_key in ti.store


def test_execute_callback_exception_is_logged(caplog):
    """Errors inside dbt callback should be logged instead of bubbling up."""

    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    op.invocation_mode = InvocationMode.DBT_RUNNER

    import cosmos.operators.watcher as _watch_mod

    if _watch_mod.EventMsg is None:

        class _DummyEv:
            pass

        eventmsg_patch = patch("cosmos.operators.watcher.EventMsg", _DummyEv, create=True)
    else:
        eventmsg_patch = nullcontext()  # type: ignore

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "dummy"}

    def fake_base_execute(self, context=None, **_):  # type: ignore[override]
        for cb in getattr(self, "_dbt_runner_callbacks", []):
            cb(_fake_event("MainReportVersion"))
        return "ok"

    with (
        eventmsg_patch,
        patch.object(DbtProducerWatcherOperator, "_handle_startup_event", side_effect=RuntimeError("boom")),
        patch("cosmos.operators.watcher.DbtLocalBaseOperator.execute", fake_base_execute),
        caplog.at_level("ERROR"),
    ):
        result = op.execute(context=ctx)

    assert result == "ok"
    assert "error while handling dbt event" in caplog.text
    assert ti.store.get("task_status") == "completed"


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


@pytest.mark.parametrize(
    "user_callback, expected_behavior",
    [
        (None, "none"),
        ([Mock(name="cb1")], "list"),
        (Mock(name="cb2"), "single"),
    ],
)
def test_set_on_failure_callback_with_actual_airflow(user_callback, expected_behavior, tmp_path):

    instance = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    result = instance._set_on_failure_callback(user_callback)

    if AIRFLOW_VERSION < Version("2.6.0"):
        # Always returns single callable regardless of input
        assert callable(result)
        assert result == instance._store_producer_task_state
    else:
        # Returns list depending on input
        assert isinstance(result, list)
        assert result[-1] == instance._store_producer_task_state

        if expected_behavior == "none":
            assert len(result) == 1
        elif expected_behavior == "list":
            assert len(result) == 2
        elif expected_behavior == "single":
            assert len(result) == 2
            assert result[0] == user_callback


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


def test_store_producer_task_state_pushes_failed_state():
    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti}
    instance = DbtProducerWatcherOperator(project_dir=".", profile_config=None)

    instance._store_producer_task_state(mock_context)

    mock_ti.xcom_push.assert_called_once_with(key="state", value="failed")


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
            deferrable=True,
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
        ti.xcom_pull.side_effect = [None, None, None]  # no event msg found
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

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._get_producer_task_state", return_value=None)
    def _fallback_to_local_run(self, mock_get_producer_task_state):
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

    def test_fallback_to_local_run(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--select", "some_model", "--threads", "2"]
        context = self.make_context(ti)
        sensor.build_and_run_cmd = MagicMock()

        result = sensor._fallback_to_local_run(2, context)

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

        result = sensor._get_status_from_run_results(ti, _MockContext(ti=ti))
        assert result == "success"

    def test_get_status_from_run_results_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = None

        result = sensor._get_status_from_run_results(ti, _MockContext(ti=ti))
        assert result is None

    def test_get_status_from_events_success(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, ENCODED_EVENT]
        context = self.make_context(ti)

        result = sensor._get_status_from_events(ti, context)
        assert result == "success"

    def test_get_status_from_events_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, None]
        context = self.make_context(ti)

        result = sensor._get_status_from_events(ti, context)
        assert result is None

    def test_get_status_from_events_sets_compiled_sql(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        event_payload = {"data": {"run_result": {"status": "success"}}, "compiled_sql": "select 42"}
        encoded_event = base64.b64encode(zlib.compress(json.dumps(event_payload).encode())).decode("utf-8")
        ti.xcom_pull.side_effect = [None, encoded_event]
        context = self.make_context(ti)

        result = sensor._get_status_from_events(ti, context)
        assert result == "success"
        assert sensor.compiled_sql == "select 42"

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._get_status_from_run_results")
    def test_producer_state_failed(self, mock_run_result):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 1
        mock_run_result.return_value = None
        ti.xcom_pull.return_value = "failed"

        context = self.make_context(ti)

        with pytest.raises(
            AirflowException,
            match="The dbt build command failed in producer task. Please check the log of task dbt_producer_watcher for details.",
        ):
            sensor.poke(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._fallback_to_local_run")
    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._get_status_from_run_results")
    def test_producer_state_does_not_fail_if_previously_upstream_failed(
        self, mock_run_result, mock_fallback_to_local_run
    ):
        """
        Attempt to run the task using ExecutionMode.LOCAL if State.UPSTREAM_FAILED happens.
        More details: https://github.com/astronomer/astronomer-cosmos/pull/2062
        """
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 0
        mock_run_result.return_value = None
        ti.xcom_pull.return_value = "failed"

        context = self.make_context(ti)

        sensor.poke(context)
        mock_fallback_to_local_run.assert_called_once()

    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_get_status_from_run_results_with_compiled_sql(self, mock_override_rtif, monkeypatch):
        sensor = self.make_sensor()
        sensor.model_unique_id = "model.test_table"

        # Create a fake run_results payload containing compiled_code and status
        run_results = {
            "results": [
                {
                    "unique_id": "model.test_table",
                    "compiled_code": "SELECT * FROM dummy_table;",
                    "status": "success",
                }
            ]
        }

        compressed = zlib.compress(json.dumps(run_results).encode())
        encoded = base64.b64encode(compressed).decode()

        # Mock TaskInstance.xcom_pull to return encoded results
        ti = MagicMock()
        ti.xcom_pull.return_value = encoded
        context = {"ti": ti}
        sensor._get_status_from_run_results(ti, context)
        mock_override_rtif.assert_called_with(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_sensor_deferred(self, mock_poke):
        mock_poke.return_value = False
        sensor = self.make_sensor()
        context = {"run_id": "run_id", "task_instance": Mock()}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, WatcherTrigger), "Trigger is not a WatcherTrigger"

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_sensor_not_deferred(self, mock_poke):
        sensor = self.make_sensor()
        sensor.deferrable = False
        context = {"run_id": "run_id", "task_instance": Mock()}
        sensor.execute(context=context)
        mock_poke.assert_called_once()

    @pytest.mark.parametrize(
        "mock_event",
        [
            {"status": "failed", "reason": "model_failed"},
            {"status": "failed", "reason": "producer_failed"},
            {"status": "success"},
        ],
    )
    def test_execute_complete(self, mock_event):
        sensor = self.make_sensor()
        if mock_event.get("status") == "failed":
            with pytest.raises(AirflowException):
                sensor.execute_complete(context=Mock(), event=mock_event)
        else:
            assert sensor.execute_complete(context=Mock(), event=mock_event) is None


class TestDbtBuildWatcherOperator:

    def test_dbt_build_watcher_operator_raises_not_implemented_error(self):
        expected_message = (
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )

        with pytest.raises(NotImplementedError, match=expected_message):
            DbtBuildWatcherOperator()


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_dag_with_watcher():
    """
    Run a DbtDag using `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
        ),
        render_config=RenderConfig(emit_datasets=False),
        operator_args={"trigger_rule": "all_success", "execution_timeout": timedelta(seconds=120)},
    )
    outcome = new_test_dag(watcher_dag)
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 23

    assert len(watcher_dag.task_dict) == 14
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = [
        "dbt_producer_watcher",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers.run",
        "stg_customers.test",
        "stg_orders.run",
        "stg_orders.test",
        "stg_payments.run",
        "stg_payments.test",
        "customers.run",
        "customers.test",
        "orders.run",
        "orders.test",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(watcher_dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_customers_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_orders_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_payments_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_customers.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_orders.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_payments.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["customers.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["orders.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_customers.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_orders.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_payments.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["customers.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["orders.test"], DbtTestWatcherOperator)

    assert watcher_dag.task_dict["dbt_producer_watcher"].downstream_task_ids == {
        "raw_payments_seed",
        "raw_orders_seed",
        "raw_customers_seed",
    }


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_task_group_with_watcher():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    from airflow import DAG

    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
    }

    with DAG(
        dag_id="example_watcher_taskgroup",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher:
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        pre_dbt = EmptyOperator(task_id="pre_dbt")

        dbt_task_group = DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

        pre_dbt
        dbt_task_group

    # Unfortunately, due to a bug in Airflow, we are not being able to set the producer task as an upstream task of the other TaskGroup tasks:
    # https://github.com/apache/airflow/issues/56723
    # When we run dag.test(), non-producer tasks are being executed before the producer task was scheduled.
    # For this reason, we are commenting out these two lines for now:
    # outcome = dag_dbt_task_group_watcher.test()
    # assert outcome.state == DagRunState.SUCCESS
    # Fortunately, when we trigger the DAG run manually, the weight is being respected and the producer task is being picked up in advance.

    assert len(dag_dbt_task_group_watcher.task_dict) == 10
    tasks_names = [task.task_id for task in dag_dbt_task_group_watcher.topological_sort()]

    expected_task_names = [
        "pre_dbt",
        "dbt_task_group.raw_customers_seed",
        "dbt_task_group.raw_orders_seed",
        "dbt_task_group.raw_payments_seed",
        "dbt_task_group.dbt_producer_watcher",
        "dbt_task_group.stg_customers_run",
        "dbt_task_group.stg_orders_run",
        "dbt_task_group.stg_payments_run",
        "dbt_task_group.customers_run",
        "dbt_task_group.orders_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"], DbtProducerWatcherOperator
    )
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_customers_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_orders_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_payments_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_customers_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_orders_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_payments_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.customers_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.orders_run"], DbtRunWatcherOperator)

    assert dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"].downstream_task_ids == set()


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_task_group_with_watcher_has_correct_dbt_cmd():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm that the dbt command flags include the expected flags.
    """
    from airflow import DAG

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    context = {"ti": MagicMock(), "run_id": "test_run_id"}

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
        "full_refresh": True,
    }

    with DAG(
        dag_id="example_watcher_taskgroup_flags",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher_flags:
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

    producer_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.dbt_producer_watcher"]
    assert producer_operator.base_cmd == ["build"]

    cmd_flags = producer_operator.add_cmd_flags()

    # Build the command without executing it
    full_cmd, env = producer_operator.build_cmd(context=context, cmd_flags=cmd_flags)

    # Verify the command was built correctly
    assert full_cmd[1] == "build"  # dbt build command
    assert "--full-refresh" in full_cmd


@pytest.mark.integration
def test_sensor_and_producer_different_param_values(mock_bigquery_conn):
    profile_mapping = get_automatic_profile_mapping(mock_bigquery_conn.conn_id, {})
    _profile_config = ProfileConfig(
        profile_name="airflow_db",
        target_name="bq",
        profile_mapping=profile_mapping,
    )
    dbt_project_path = Path(__file__).parent.parent.parent / "dev/dags/dbt"

    dag = DbtDag(
        project_config=ProjectConfig(dbt_project_path=dbt_project_path / "jaffle_shop"),
        profile_config=_profile_config,
        operator_args={
            "install_deps": True,
            "full_refresh": True,
            "deferrable": False,
            "execution_timeout": timedelta(seconds=1),
        },
        render_config=RenderConfig(test_behavior=TestBehavior.NONE),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER, setup_operator_args={"execution_timeout": timedelta(seconds=2)}
        ),
        schedule="@daily",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        dag_id="test_sensor_args_import",
    )

    for task in dag.tasks_map.values():
        if isinstance(task, DbtProducerWatcherOperator):
            assert task.execution_timeout == timedelta(seconds=2)
        else:
            assert task.execution_timeout == timedelta(seconds=1)
