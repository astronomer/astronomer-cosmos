from __future__ import annotations

import base64
import json
import logging
import zlib
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.state import DagRunState

try:
    from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_QUEUE
except ImportError:  # pragma: no cover
    from airflow.models.abstractoperator import DEFAULT_QUEUE  # type: ignore[no-redef]

from packaging.version import Version

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, TestBehavior
from cosmos.config import InvocationMode
from cosmos.constants import PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT, ExecutionMode
from cosmos.operators._watcher.base import store_compiled_sql_for_model
from cosmos.operators._watcher.triggerer import WatcherTrigger
from cosmos.operators.watcher import (
    DbtBuildWatcherOperator,
    DbtConsumerWatcherSensor,
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtTestWatcherOperator,
    store_dbt_resource_status_from_log,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping, get_automatic_profile_mapping
from tests.utils import AIRFLOW_VERSION, new_test_dag

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"

DBT_EXECUTABLE_PATH = Path(__file__).parent.parent.parent / "venv-subprocess/bin/dbt"
DBT_PROJECT_WITH_EMPTY_MODEL_PATH = Path(__file__).parent.parent / "sample/dbt_project_with_empty_model"

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
    assert op.priority_weight == PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT


@pytest.mark.parametrize(
    "queue_override, expected_queue",
    [
        ("custom_retry_queue", "custom_retry_queue"),
        (None, DEFAULT_QUEUE),
    ],
)
def test_dbt_producer_watcher_operator_queue(queue_override, expected_queue):
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", queue_override):
        op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)

        assert op.queue == expected_queue


@pytest.mark.integration
def test_producer_queue_from_setup_operator_args_when_both_set():
    """
    When both setup_operator_args queue and watcher_dbt_execution_queue are set,
    producer should use queue from setup_operator_args.
    """
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", "watcher_queue"):
        watcher_dag = DbtDag(
            project_config=project_config,
            profile_config=profile_config,
            start_date=datetime(2023, 1, 1),
            dag_id="watcher_dag_setup_queue",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
                setup_operator_args={"queue": "dbt_producer_task_queue"},
            ),
            render_config=RenderConfig(emit_datasets=False),
        )
    producer = watcher_dag.task_dict["dbt_producer_watcher"]
    assert producer.queue == "dbt_producer_task_queue"


@pytest.mark.integration
def test_producer_queue_from_watcher_dbt_execution_queue_when_only_watcher_set():
    """
    When only watcher_dbt_execution_queue is set (no queue in setup_operator_args),
    producer should use queue from watcher_dbt_execution_queue.
    """
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", "watcher_only_queue"):
        watcher_dag = DbtDag(
            project_config=project_config,
            profile_config=profile_config,
            start_date=datetime(2023, 1, 1),
            dag_id="watcher_dag_watcher_queue_only",
            execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
            render_config=RenderConfig(emit_datasets=False),
        )
    producer = watcher_dag.task_dict["dbt_producer_watcher"]
    assert producer.queue == "watcher_only_queue"


def test_dbt_producer_watcher_operator_priority_weight_override():
    """Test that DbtProducerWatcherOperator allows overriding priority_weight."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, priority_weight=100)
    assert op.priority_weight == 100


def test_dbt_producer_watcher_operator_retries_forced_to_zero():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.retries == 0


@pytest.mark.parametrize(
    "invocation_mode, expected_log_format",
    (
        (InvocationMode.SUBPROCESS, "json"),
        (InvocationMode.DBT_RUNNER, None),
    ),
)
def test_dbt_producer_log_format_adjusts_with_invocation(invocation_mode, expected_log_format):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, invocation_mode=invocation_mode)
    assert getattr(op, "log_format", None) == expected_log_format


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


def test_dbt_consumer_watcher_sensor_execute_complete_model_not_run_logs_message(caplog):
    """Test that execute_complete logs an info message when model was skipped (model_not_run)."""
    sensor = DbtConsumerWatcherSensor(
        project_dir=".",
        profiles_dir=".",
        profile_config=profile_config,
        model_unique_id="model.pkg.skipped_model",
        poke_interval=1,
        producer_task_id="dbt_producer_watcher_operator",
        task_id="consumer_sensor",
    )
    sensor.model_unique_id = "model.pkg.skipped_model"

    context = {"dag_run": MagicMock()}
    event = {"status": "success", "reason": "model_not_run"}

    with caplog.at_level(logging.INFO):
        sensor.execute_complete(context, event)

    assert any(
        "Model 'model.pkg.skipped_model' was skipped by the dbt command" in message for message in caplog.messages
    )
    assert any("ephemeral model or if the model sql file is empty" in message for message in caplog.messages)


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


def test_dbt_producer_watcher_operator_skips_retry_attempt(caplog):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ti.try_number = 2
    context = {"ti": ti}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        with caplog.at_level(logging.INFO):
            result = op.execute(context=context)

    mock_execute.assert_not_called()
    assert result is None
    assert any("does not support Airflow retries" in message for message in caplog.messages)
    assert any("skipping execution" in message for message in caplog.messages)


@pytest.mark.parametrize(
    "event, expected_message",
    [
        ({"status": "success"}, None),
        ({"status": "success", "reason": "model_not_run"}, None),
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

    # Compiled SQL is pushed to the canonical XCom key (single strategy for both event and subprocess)
    assert ti.store.get("model__pkg__my_model_compiled_sql") == sql_text


def test_handle_node_finished_without_compiled_sql_does_not_inject(tmp_path, monkeypatch):
    op = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    # Ensure watcher looks up under this tmp project dir, but do NOT create compiled file
    monkeypatch.chdir(tmp_path)

    with patch.object(op, "_serialize_event", return_value={}):
        ev = _fake_event(name="NodeFinished", uid="model.pkg.my_model", resource_type="model", node_path="my_model.sql")
        op._handle_node_finished(ev, ctx)

    # Event payload does not contain compiled_sql; canonical key is only set when extraction succeeds
    stored = list(ti.store.values())[0]
    data = json.loads(zlib.decompress(base64.b64decode(stored)).decode())
    assert "compiled_sql" not in data
    assert "model__pkg__my_model_compiled_sql" not in ti.store


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


class TestStoreDbtStatusFromLog:
    """Tests for store_dbt_resource_status_from_log and _process_log_line_callable."""

    def test_store_dbt_resource_status_from_log_success(self):
        """Test that success status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"node_info": {"node_status": "success", "unique_id": "model.pkg.my_model"}}})

        store_dbt_resource_status_from_log(log_line, {"context": ctx})

        assert ti.store.get("model__pkg__my_model_status") == "success"

    def test_store_dbt_resource_status_from_log_failed(self):
        """Test that failed status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"node_info": {"node_status": "failed", "unique_id": "model.pkg.failed_model"}}})

        store_dbt_resource_status_from_log(log_line, {"context": ctx})

        assert ti.store.get("model__pkg__failed_model_status") == "failed"

    def test_store_dbt_resource_status_from_log_ignores_other_statuses(self):
        """Test that statuses other than success/failed are ignored."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps(
            {"data": {"node_info": {"node_status": "running", "unique_id": "model.pkg.running_model"}}}
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx})

        assert "model__pkg__running_model_status" not in ti.store

    def test_store_dbt_resource_status_from_log_handles_invalid_json(self, caplog):
        """Test that invalid JSON doesn't raise an exception."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Should not raise an exception
        store_dbt_resource_status_from_log("not valid json {{{", {"context": ctx})

        # No status should be stored
        assert len(ti.store) == 0

    def test_store_dbt_resource_status_from_log_handles_missing_node_info(self):
        """Test that missing node_info doesn't raise an exception."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"other_key": "value"}})

        # Should not raise an exception
        store_dbt_resource_status_from_log(log_line, {"context": ctx})

        # No status should be stored
        assert len(ti.store) == 0

    @pytest.mark.parametrize(
        "msg, level",
        [
            ("Running with dbt=1.10.11", "info"),
            ("This is a warning", "warning"),
            ("An error occurred", "error"),
            ("Debugging info", "debug"),
            ("Unknown level defaults to INFO", "unknown"),  # just to ensure it defaults
        ],
    )
    def test_store_dbt_resource_status_from_log_outputs_dbt_info(self, caplog, msg, level):
        """Test that dbt info messages are logged correctly."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"info": {"msg": msg, "level": level}})
        dynamic_level = getattr(logging, level.upper(), logging.INFO)
        with caplog.at_level(dynamic_level):
            store_dbt_resource_status_from_log(log_line, {"context": ctx})

        assert msg in caplog.text
        assert any(record.levelname == logging.getLevelName(dynamic_level) for record in caplog.records)

    def test_store_dbt_resource_status_from_log_logs_message_only_once(self, caplog):
        """Test that dbt log messages are logged exactly once (no duplicates)."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "1 of 5 START sql view model release_17.stg_customers"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": "2025-01-29T13:16:05.123456Z"}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(log_line, {"context": ctx})

        # Count how many times the message appears in log records
        message_count = sum(1 for record in caplog.records if test_msg in record.message)
        assert message_count == 1, f"Expected message to be logged exactly once, but found {message_count} times"

    def test_store_dbt_resource_status_from_log_formats_timestamp(self, caplog):
        """Test that the timestamp is formatted as HH:MM:SS to match dbt runner format."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "Running with dbt=1.10.11"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": "2025-01-29T13:16:05.123456Z"}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(log_line, {"context": ctx})

        # Verify the timestamp is formatted as HH:MM:SS
        assert any("13:16:05" in record.message and test_msg in record.message for record in caplog.records)

    def test_store_dbt_resource_status_from_log_invalid_timestamp_falls_back_to_raw(self, caplog):
        """Test that invalid timestamps fall back to raw value instead of raising an error."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "Running with dbt=1.10.11"
        # Looks like a valid ISO timestamp but has invalid month (13) - triggers ValueError in fromisoformat()
        invalid_ts = "2025-13-29T13:16:05.123456Z"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": invalid_ts}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(log_line, {"context": ctx})

        # Verify the raw timestamp is used when parsing fails
        assert any(invalid_ts in record.message and test_msg in record.message for record in caplog.records)

    def test_process_log_line_callable_integration_with_subprocess_pattern(self):
        """Test the exact pattern used in subprocess.py: process_log_line(line, kwargs)."""
        op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
        op._process_log_line_callable = store_dbt_resource_status_from_log

        ti = _MockTI()
        ctx = {"ti": ti}

        # Simulate the kwargs dict that subprocess.py passes
        kwargs = {"context": ctx, "other_param": "value"}

        log_lines = [
            json.dumps({"data": {"node_info": {"node_status": "success", "unique_id": "model.pkg.model_a"}}}),
            json.dumps({"data": {"node_info": {"node_status": "failed", "unique_id": "model.pkg.model_b"}}}),
            json.dumps({"info": {"msg": "Running with dbt=1.10.11"}}),  # Non-node log line
        ]

        # Simulate the subprocess.py pattern
        for line in log_lines:
            op._process_log_line_callable(line, kwargs)

        assert ti.store.get("model__pkg__model_a_status") == "success"
        assert ti.store.get("model__pkg__model_b_status") == "failed"
        assert len(ti.store) == 2  # Only success and failed statuses are stored

    def test_store_dbt_resource_status_from_log_pushes_compiled_sql_for_models(self, tmp_path):
        """Test that compiled_sql is pushed to XCom for successful model nodes."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Create a fake compiled SQL file
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        compiled_sql_file = compiled_dir / "my_model.sql"
        compiled_sql_file.write_text("SELECT * FROM orders WHERE status = 'completed'")

        # JSON log with node_path and resource_type
        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "success",
                        "unique_id": "model.pkg.my_model",
                        "node_path": "my_model.sql",
                        "resource_type": "model",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("model__pkg__my_model_status") == "success"
        assert ti.store.get("model__pkg__my_model_compiled_sql") == "SELECT * FROM orders WHERE status = 'completed'"

    def test_store_dbt_resource_status_from_log_no_compiled_sql_for_non_models(self, tmp_path):
        """Test that compiled_sql is not pushed for non-model resources like seeds or tests."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # JSON log for a seed (not a model)
        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "success",
                        "unique_id": "seed.pkg.my_seed",
                        "node_path": "my_seed.csv",
                        "resource_type": "seed",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("seed__pkg__my_seed_status") == "success"
        assert "seed__pkg__my_seed_compiled_sql" not in ti.store

    def test_store_dbt_resource_status_from_log_pushes_compiled_sql_on_failure(self, tmp_path):
        """Test that compiled_sql is pushed for failed models too (useful for debugging)."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Create a fake compiled SQL file (compilation happens before execution, so it exists even if model fails)
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        compiled_sql_file = compiled_dir / "failed_model.sql"
        compiled_sql_file.write_text("SELECT * FROM orders WHERE invalid_column = 1")

        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "failed",
                        "unique_id": "model.pkg.failed_model",
                        "node_path": "failed_model.sql",
                        "resource_type": "model",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("model__pkg__failed_model_status") == "failed"
        assert ti.store.get("model__pkg__failed_model_compiled_sql") == "SELECT * FROM orders WHERE invalid_column = 1"


class TestStoreCompiledSqlForModelPathHandling:
    """Tests for store_compiled_sql_for_model (node_path is relative to target/compiled/<package>/models/)."""

    def test_missing_node_path_does_not_push(self, tmp_path):
        """When node_path is None or empty, no compiled_sql is extracted or pushed."""
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.m", None, "model")
        assert "model__pkg__m_compiled_sql" not in ti.store

        ti2 = _MockTI()
        store_compiled_sql_for_model(ti2, str(tmp_path), "model.pkg.m", "", "model")
        assert "model__pkg__m_compiled_sql" not in ti2.store

    def test_nonexistent_path_does_not_push(self, tmp_path):
        """When compiled SQL path does not exist, we do not push."""
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.my_model", "nonexistent.sql", "model")
        assert "model__pkg__my_model_compiled_sql" not in ti.store

    def test_path_traversal_outside_project_does_not_push(self, tmp_path):
        """node_path with .. that resolves outside compiled root: file does not exist, so we do not push."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "legit.sql").write_text("SELECT 1")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.m", "../../../etc/passwd", "model")
        assert "model__pkg__m_compiled_sql" not in ti.store

    def test_compiled_sql_under_models_pushed(self, tmp_path):
        """node_path relative to models/ (e.g. staging/foo.sql) is read and pushed."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models" / "staging"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "stg_orders.sql").write_text("SELECT * FROM staging.orders")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.stg_orders", "staging/stg_orders.sql", "model")
        assert ti.store.get("model__pkg__stg_orders_compiled_sql") == "SELECT * FROM staging.orders"

    def test_compiled_sql_flat_path_pushed(self, tmp_path):
        """node_path as single file under models/ (e.g. foo.sql) is read and pushed."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "foo.sql").write_text("SELECT 1")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.foo", "foo.sql", "model")
        assert ti.store.get("model__pkg__foo_compiled_sql") == "SELECT 1"


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
            deferrable=kwargs.pop("deferrable", True),
            **kwargs,
        )

        sensor.invocation_mode = "DBT_RUNNER"
        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    def make_context(self, ti_mock, *, run_id: str = "test-run", map_index: int = 0):
        return {
            "ti": ti_mock,
            "run_id": run_id,
            "task_instance": MagicMock(map_index=map_index),
        }

    @pytest.mark.skipif(AIRFLOW_VERSION >= Version("3.0.0"), reason="RuntimeTaskInstance path in Airflow >= 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("2.9.0"))
    def test_get_producer_task_status_airflow2(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_1")

        fetcher = MagicMock(return_value="success")

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=fetcher) as mock_builder:
            status = sensor._get_producer_task_status(context)

        mock_builder.assert_called_once_with(
            airflow_version=Version("2.9.0"),
            dag_id="example_dag",
            run_id="run_1",
            producer_task_id=sensor.producer_task_id,
            logger=ANY,
        )
        fetcher.assert_called_once_with()
        assert status == "success"

    @pytest.mark.skipif(AIRFLOW_VERSION >= Version("3.0.0"), reason="RuntimeTaskInstance path in Airflow >= 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("2.9.0"))
    def test_get_producer_task_status_airflow2_missing_instance(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_2")

        fetcher = MagicMock(return_value=None)

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=fetcher):
            status = sensor._get_producer_task_status(context)

        fetcher.assert_called_once_with()
        assert status is None

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    @patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states")
    def test_get_producer_task_status_airflow3(self, mock_get_task_states):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_3")

        mock_get_task_states.return_value = {"run_3": {sensor.producer_task_id: "running"}}

        status = sensor._get_producer_task_status(context)

        assert status == "running"
        mock_get_task_states.assert_called_once_with(
            dag_id="example_dag", task_ids=[sensor.producer_task_id], run_ids=["run_3"]
        )

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    @patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states")
    def test_get_producer_task_status_airflow3_missing_state(self, mock_get_task_states):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_3_missing")

        mock_get_task_states.return_value = {"run_3_missing": {}}

        status = sensor._get_producer_task_status(context)

        assert status is None
        mock_get_task_states.assert_called_once_with(
            dag_id="example_dag", task_ids=[sensor.producer_task_id], run_ids=["run_3_missing"]
        )

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    def test_get_producer_task_status_airflow3_import_error(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_4")

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=None) as mock_builder:
            status = sensor._get_producer_task_status(context)

        mock_builder.assert_called_once_with(
            airflow_version=Version("3.0.0"),
            dag_id="example_dag",
            run_id="run_4",
            producer_task_id=sensor.producer_task_id,
            logger=ANY,
        )
        assert status is None

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
        ti.xcom_pull.return_value = "success"
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_poke_subprocess_mode_extracts_compiled_sql_from_xcom(self, mock_override_rtif):
        """Test that in subprocess mode, poke extracts compiled_sql from per-model XCom key when status is success."""
        sensor = self.make_sensor()
        sensor.invocation_mode = "SUBPROCESS"
        sensor.model_unique_id = MODEL_UNIQUE_ID

        ti = MagicMock()
        ti.try_number = 1
        # First call returns status from per-model XCom key, second call returns compiled_sql from per-model XCom key
        ti.xcom_pull.side_effect = ["success", "SELECT * FROM orders"]
        context = self.make_context(ti)

        assert sensor.compiled_sql == ""  # Initially empty
        result = sensor.poke(context)
        assert result is True
        assert sensor.compiled_sql == "SELECT * FROM orders"
        mock_override_rtif.assert_called_once_with(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.use_event", return_value=True)
    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._get_producer_task_status", return_value="running")
    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_poke_event_mode_extracts_compiled_sql_from_canonical_key(
        self, mock_override_rtif, mock_get_producer, mock_use_event
    ):
        """Test that in event (DBT_RUNNER) mode, poke gets compiled_sql from canonical *_compiled_sql key after status."""
        sensor = self.make_sensor()
        sensor.model_unique_id = MODEL_UNIQUE_ID

        ti = MagicMock()
        ti.try_number = 1
        # _get_status_from_events: dbt_startup_events=None, nodefinished_*=ENCODED_EVENT; then get_xcom_val(compiled_sql_key)
        ti.xcom_pull.side_effect = [None, ENCODED_EVENT, "SELECT * FROM orders"]
        context = self.make_context(ti)

        assert sensor.compiled_sql == ""  # Initially empty
        result = sensor.poke(context)
        assert result is True
        assert sensor.compiled_sql == "SELECT * FROM orders"
        mock_override_rtif.assert_called_once_with(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._get_producer_task_status", return_value=None)
    def _fallback_to_non_watcher_run(self, mock_get_producer_task_status):
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

    def test_fallback_to_non_watcher_run(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--select", "some_model", "--threads", "2"]
        context = self.make_context(ti)
        sensor.build_and_run_cmd = MagicMock()

        result = sensor._fallback_to_non_watcher_run(2, context)

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

    def test_get_status_from_events_does_not_set_compiled_sql_from_event(self):
        """compiled_sql is no longer in the event payload; it is read from the canonical XCom key in poke()."""
        sensor = self.make_sensor()
        ti = MagicMock()
        event_payload = {"data": {"run_result": {"status": "success"}}}
        encoded_event = base64.b64encode(zlib.compress(json.dumps(event_payload).encode())).decode("utf-8")
        ti.xcom_pull.side_effect = [None, encoded_event]
        context = self.make_context(ti)

        result = sensor._get_status_from_events(ti, context)
        assert result == "success"
        assert sensor.compiled_sql == ""  # not set from event; poke() will get it from canonical key

    @patch("cosmos.operators._watcher.base.get_xcom_val")
    def test_producer_state_failed(self, mock_get_xcom_val):
        sensor = self.make_sensor()
        sensor._get_producer_task_status.return_value = "failed"
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 1
        mock_get_xcom_val.return_value = None
        ti.xcom_pull.return_value = "failed"

        context = self.make_context(ti)

        with pytest.raises(
            AirflowException,
            match="The dbt build command failed in producer task. Please check the log of task dbt_producer_watcher for details.",
        ):
            sensor.poke(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._fallback_to_non_watcher_run")
    @patch("cosmos.operators._watcher.base.get_xcom_val")
    def test_producer_state_does_not_fail_if_previously_upstream_failed(
        self, mock_get_xcom_val, mock_fallback_to_non_watcher_run
    ):
        """
        Attempt to run the task using ExecutionMode.LOCAL if State.UPSTREAM_FAILED happens.
        More details: https://github.com/astronomer/astronomer-cosmos/pull/2062
        """
        sensor = self.make_sensor()
        sensor._get_producer_task_status.return_value = "failed"
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 0
        mock_get_xcom_val.return_value = None
        ti.xcom_pull.return_value = "failed"

        context = self.make_context(ti)

        sensor.poke(context)
        mock_fallback_to_non_watcher_run.assert_called_once()

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

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_deferrable_false_via_constructor_does_not_defer(self, mock_poke):
        """operator_args={'deferrable': False} is respected: sensor created with deferrable=False does not defer."""
        mock_poke.return_value = True
        sensor = self.make_sensor(deferrable=False)
        assert sensor.deferrable is False
        context = {"run_id": "run_id", "task_instance": Mock()}
        sensor.execute(context=context)
        mock_poke.assert_called_once()
        # No TaskDeferred raised: sensor ran synchronously and completed

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

    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_execute_complete_extracts_compiled_sql(self, mock_override_rtif):
        """Test that execute_complete extracts compiled_sql from the event and sets it on the sensor."""
        sensor = self.make_sensor()
        context = Mock()

        assert sensor.compiled_sql == ""  # Initially empty

        event = {"status": "success", "compiled_sql": "SELECT * FROM orders WHERE status = 'active'"}
        sensor.execute_complete(context=context, event=event)

        assert sensor.compiled_sql == "SELECT * FROM orders WHERE status = 'active'"
        mock_override_rtif.assert_called_once_with(context)

    def test_execute_complete_handles_missing_compiled_sql(self):
        """Test that execute_complete handles events without compiled_sql gracefully."""
        sensor = self.make_sensor()
        context = Mock()

        assert sensor.compiled_sql == ""  # Initially empty

        event = {"status": "success"}  # No compiled_sql in event
        sensor.execute_complete(context=context, event=event)

        assert sensor.compiled_sql == ""  # Should remain empty


class TestDbtBuildWatcherOperator:
    def test_dbt_build_watcher_operator_raises_not_implemented_error(self):
        expected_message = (
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )

        with pytest.raises(NotImplementedError, match=expected_message):
            DbtBuildWatcherOperator()


class TestWatcherTrigger:
    """Tests for WatcherTrigger compiled_sql extraction."""

    def make_trigger(self, use_event: bool = False):
        return WatcherTrigger(
            model_unique_id="model.pkg.my_model",
            producer_task_id="dbt_producer_watcher",
            dag_id="test_dag",
            run_id="test_run",
            map_index=None,
            use_event=use_event,
            poke_interval=1.0,
        )

    @pytest.mark.asyncio
    async def test_parse_node_status_and_compiled_sql_subprocess_mode(self):
        """Test that compiled_sql is extracted from XCom in subprocess mode."""
        trigger = self.make_trigger(use_event=False)

        # Mock get_xcom_val to return status and compiled_sql
        async def mock_get_xcom_val(key):
            if key == "model__pkg__my_model_status":
                return "success"
            elif key == "model__pkg__my_model_compiled_sql":
                return "SELECT * FROM orders"
            return None

        trigger.get_xcom_val = mock_get_xcom_val

        status, compiled_sql = await trigger._parse_node_status_and_compiled_sql()

        assert status == "success"
        assert compiled_sql == "SELECT * FROM orders"

    @pytest.mark.asyncio
    async def test_parse_node_status_and_compiled_sql_subprocess_no_compiled_sql(self):
        """Test that missing compiled_sql is handled gracefully in subprocess mode."""
        trigger = self.make_trigger(use_event=False)

        # Mock get_xcom_val to return only status
        async def mock_get_xcom_val(key):
            if key == "model__pkg__my_model_status":
                return "success"
            return None

        trigger.get_xcom_val = mock_get_xcom_val

        status, compiled_sql = await trigger._parse_node_status_and_compiled_sql()

        assert status == "success"
        assert compiled_sql is None

    @pytest.mark.asyncio
    async def test_parse_node_status_and_compiled_sql_dbt_runner_mode(self):
        """Test that in dbt_runner mode status comes from event payload and compiled_sql from canonical key."""
        trigger = self.make_trigger(use_event=True)

        # Event payload (no longer contains compiled_sql; it is stored under canonical key)
        event_data = {"data": {"run_result": {"status": "success"}}}
        compressed = base64.b64encode(zlib.compress(json.dumps(event_data).encode())).decode()

        async def mock_get_xcom_val(key):
            if key == "nodefinished_model__pkg__my_model":
                return compressed
            if key == "model__pkg__my_model_compiled_sql":
                return "SELECT id FROM users"
            return None

        trigger.get_xcom_val = mock_get_xcom_val

        status, compiled_sql = await trigger._parse_node_status_and_compiled_sql()

        assert status == "success"
        assert compiled_sql == "SELECT id FROM users"


@pytest.mark.integration
def test_dbt_dag_with_watcher(capsys):
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
            invocation_mode=InvocationMode.DBT_RUNNER,
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

    # dbt runner logs are not captured by caplog, so we need to capture them using capsys
    capsys_output = capsys.readouterr()
    stdout = capsys_output.out

    assert (
        '''"node_status": "success", "resource_type": "seed", "unique_id": "seed.jaffle_shop.raw_orders"'''
        not in stdout
    )

    log_message = "OK loaded seed file public.raw_orders"
    assert log_message in stdout

    # Verify that log messages are not duplicated (each dbt message should appear only once)
    message_count = stdout.count(log_message)
    assert message_count == 1, f"Expected '{log_message}' to be logged exactly once, but found {message_count} times"


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_dag_with_watcher_and_subprocess(caplog):
    """
    Run a DbtDag using `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag_with_subprocess",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(emit_datasets=False, select=["raw_orders"], test_behavior=TestBehavior.AFTER_ALL),
        operator_args={"trigger_rule": "all_success", "execution_timeout": timedelta(seconds=120)},
    )
    dag_run = new_test_dag(watcher_dag)
    assert dag_run.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 1

    assert len(watcher_dag.task_dict) == 3
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = ["dbt_producer_watcher", "raw_orders_seed", "jaffle_shop_test"]
    assert tasks_names == expected_task_names
    # Confirm that the dbt command was successfully run using the given dbt executable path:
    assert "venv-subprocess/bin/dbt'), 'build'" in caplog.text
    # Confirm that the seed was successfully run and the log output was JSON:
    assert (
        '''"node_status": "success", "resource_type": "seed", "unique_id": "seed.jaffle_shop.raw_orders"'''
        not in caplog.text
    )

    log_message = "OK loaded seed file public.raw_orders"
    assert log_message in caplog.text

    # Verify that log messages are not duplicated (each dbt message should appear only once)
    message_count = sum(1 for record in caplog.records if log_message in record.message)
    assert message_count == 1, f"Expected '{log_message}' to be logged exactly once, but found {message_count} times"


# Airflow 3.0.0 hangs indefinitely, while Airflow 3.0.6 fails due to this Airflow bug:
# https://github.com/apache/airflow/issues/51816
@pytest.mark.skipif(
    AIRFLOW_VERSION == Version("3.0"),
    reason="Airflow hangs in these versions when trying to fetch XCom from the triggerer when using dags.test()",
)
@pytest.mark.integration
def test_dbt_dag_with_watcher_and_empty_model(caplog):
    """
    Run a DbtDag using `ExecutionMode.WATCHER` and a dbt project with an empty model. This was a situation observed by an Astronomer customer.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_WITH_EMPTY_MODEL_PATH,
    )
    # There are two dbt projects defined in this folder.
    # When we run `dbt ls`, we can see this:
    #
    # 10:32:30  Found 2 models, 464 macros
    # micro_dbt_project.add_row
    # micro_dbt_project.empty_model
    #
    # However, during `dbt build`, dbt skips running the empty model, and only runs the add_row model:
    #
    # 10:29:03  Running with dbt=1.11.2
    # 10:29:03  Registered adapter: postgres=1.10.0
    # 10:29:03  Found 2 models, 464 macros
    # 10:29:03
    # 10:29:03  Concurrency: 4 threads (target='dev')
    # 10:29:03
    # 10:29:03  1 of 1 START sql view model public.add_row ..................................... [RUN]
    # 10:29:03  1 of 1 OK created sql view model public.add_row ................................ [CREATE VIEW in 0.06s]
    # 10:29:03
    # 10:29:03  Finished running 1 view model in 0 hours 0 minutes and 0.19 seconds (0.19s).
    # 10:29:03
    # 10:29:03  Completed successfully
    # 10:29:03
    # 10:29:03  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1

    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag_empty_model",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            invocation_mode=InvocationMode.DBT_RUNNER,
        ),
        render_config=RenderConfig(emit_datasets=False, test_behavior=TestBehavior.NONE),
        operator_args={
            "trigger_rule": "all_success",
            "execution_timeout": timedelta(seconds=10),
        },
        dagrun_timeout=timedelta(seconds=30),
    )
    outcome = new_test_dag(watcher_dag)
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 2

    assert len(watcher_dag.task_dict) == 3
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = [
        "dbt_producer_watcher",
        "add_row_run",
        "empty_model_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(watcher_dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict["add_row_run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["empty_model_run"], DbtRunWatcherOperator)

    assert watcher_dag.task_dict["dbt_producer_watcher"].downstream_task_ids == {
        "add_row_run",
        "empty_model_run",
    }

    assert "Total filtered nodes: 2" in caplog.text
    assert "Finished running node model.micro_dbt_project.add_row" in caplog.text
    assert "Finished running node model.micro_dbt_project.empty_model_run" not in caplog.text
    assert "Model 'model.micro_dbt_project.empty_model' was skipped by the dbt command" in caplog.text


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
        "dbt_task_group.dbt_producer_watcher",
        "dbt_task_group.raw_customers_seed",
        "dbt_task_group.raw_orders_seed",
        "dbt_task_group.raw_payments_seed",
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
def test_dbt_task_group_with_watcher_has_correct_templated_dbt_cmd():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm that the dbt commands for both producer and sensor tasks include the expected templated flags.
    """
    from airflow import DAG

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    context = {"ti": MagicMock(try_number=1), "run_id": "test_run_id"}

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
        "full_refresh": True,
        "dbt_cmd_flags": ["--threads", "{{ 1 if ti.try_number > 1 else 'x' }}"],
    }

    with DAG(
        dag_id="example_watcher_taskgroup_flags",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher_flags:
        """
        Example DAG using a DbtTaskGroup with ExecutionMode.WATCHER, validating that templated dbt command
        flags are rendered and passed correctly to both producer and sensor tasks.
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

    # Basic check for producer task
    producer_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.dbt_producer_watcher"]
    producer_operator.render_template_fields(context=context)  # Render the templated fields
    assert producer_operator.base_cmd == ["build"]

    # Build the command without executing it and verify it was built correctly
    cmd_flags = producer_operator.add_cmd_flags()
    full_cmd, _ = producer_operator.build_cmd(context=context, cmd_flags=cmd_flags)
    assert full_cmd[1] == "build"  # dbt build command

    cmd = " ".join(full_cmd)
    assert "--full-refresh" in full_cmd
    assert "--threads x" in cmd

    # Setup for checking the sensor task, which has templated command flags
    context["ti"].task.dag.get_task.return_value = producer_operator
    context["ti"].try_number = 2
    sensor_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.stg_customers_run"]
    sensor_operator.render_template_fields(context=context)  # Render the templated fields
    assert sensor_operator.base_cmd == ["run"]

    # Build the command without executing it and verify it was built correctly
    cmd_flags = sensor_operator.add_cmd_flags()
    full_cmd, _ = sensor_operator.build_cmd(context=context, cmd_flags=cmd_flags)
    assert full_cmd[1] == "run"  # dbt run command

    cmd = " ".join(full_cmd)
    assert "--select fqn:jaffle_shop.staging.stg_customers" in cmd
    assert "--threads 1" in cmd


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


def test_dbt_source_watcher_operator_template_fields():
    """Test that DbtSourceWatcherOperator doesn't include model_unique_id in template_fields."""
    from cosmos.operators.local import DbtSourceLocalOperator
    from cosmos.operators.watcher import DbtSourceWatcherOperator

    # DbtSourceWatcherOperator should NOT have model_unique_id in template_fields
    # because it runs locally and doesn't watch models, it executes source freshness
    assert "model_unique_id" not in DbtSourceWatcherOperator.template_fields

    # DbtSourceWatcherOperator should inherit template_fields from DbtSourceLocalOperator
    assert DbtSourceWatcherOperator.template_fields == DbtSourceLocalOperator.template_fields
