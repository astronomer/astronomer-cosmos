from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from cosmos.constants import InvocationMode
from cosmos.listeners import task_instance_listener
from cosmos.operators.base import AbstractDbtBase


class DummyDbtOperator(AbstractDbtBase):
    base_cmd = ["run"]

    def __init__(
        self,
        *,
        module: str = "cosmos.operators.local.fake",
        install_deps: bool | None = True,
        callback=None,
        runner_callbacks=None,
    ) -> None:
        super().__init__(project_dir="/tmp")
        self.invocation_mode = InvocationMode.DBT_RUNNER
        self._task_module = module
        if install_deps is not None:
            self.install_deps = install_deps
        if callback is not None:
            self.callback = callback
        if runner_callbacks is not None:
            self._dbt_runner_callbacks = runner_callbacks

    def build_and_run_cmd(
        self, context, cmd_flags, run_as_async=False, async_context=None, **kwargs
    ):  # pragma: no cover
        return None


class DummyDbtOperatorNoDeps(DummyDbtOperator):
    base_cmd = ["seed"]

    def __init__(self) -> None:
        super().__init__(module="cosmos.operators.kubernetes.fake", install_deps=None)
        self.invocation_mode = InvocationMode.SUBPROCESS
        if hasattr(self, "install_deps"):
            delattr(self, "install_deps")


class CustomDbtSubclass(DummyDbtOperator):
    def __init__(self) -> None:
        super().__init__(module="custom.pipeline.dummy")


class DummyDbtOperatorNoCommand(DummyDbtOperator):
    base_cmd = None


class DummyDbtOperatorStringCommand(DummyDbtOperator):
    base_cmd = "deps"


class DummyDbtOperatorTupleCommand(DummyDbtOperator):
    base_cmd = ("run", None, "--full-refresh")


class NonCosmosOperator:
    __module__ = "airflow.operators.bash"

    def __init__(self) -> None:
        self._task_module = "airflow.operators.bash"


def _make_task_instance(task, **overrides) -> SimpleNamespace:
    defaults = dict(
        dag_id="example_dag",
        task_id="example_task",
        task=task,
        queue="default",
        priority_weight=5,
        map_index=-1,
        dag_run=SimpleNamespace(run_id="run-1", dag_hash="hash-123"),
        duration=7.0,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_build_task_metrics_records_core_fields():
    operator = DummyDbtOperator()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["operator_name"] == "DummyDbtOperator"
    assert metrics["dbt_command"] == "run"
    assert metrics["install_deps"] is True
    assert metrics["invocation_mode"] == InvocationMode.DBT_RUNNER.value
    assert metrics["execution_mode"] == "local"
    assert metrics["is_cosmos_operator_subclass"] is False
    assert metrics["dag_run_id"] == "run-1"
    assert metrics["dag_hash"] == "hash-123"


def test_build_task_metrics_ignores_missing_install_deps():
    operator = DummyDbtOperatorNoDeps()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="failed")

    assert metrics["dbt_command"] == "seed"
    assert "install_deps" not in metrics
    assert metrics["execution_mode"] == "kubernetes"


def test_build_task_metrics_marks_custom_subclasses():
    operator = CustomDbtSubclass()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["is_cosmos_operator_subclass"] is True
    assert metrics["execution_mode"] is None
    assert metrics["has_callback"] is False


def test_build_task_metrics_sets_has_callback_for_callable():
    operator = DummyDbtOperator(callback=lambda *_: None)
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["has_callback"] is True


def test_build_task_metrics_interprets_tuple_callbacks():
    operator = DummyDbtOperator(callback=(None, lambda *_: None))
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["has_callback"] is True


def test_build_task_metrics_skips_dbt_command_when_missing():
    operator = DummyDbtOperatorNoCommand()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert "dbt_command" not in metrics


def test_build_task_metrics_handles_string_dbt_command():
    operator = DummyDbtOperatorStringCommand()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["dbt_command"] == "deps"


def test_build_task_metrics_flattens_iterable_commands():
    operator = DummyDbtOperatorTupleCommand()
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["dbt_command"] == "run --full-refresh"


def test_build_task_metrics_handles_missing_invocation_mode():
    operator = DummyDbtOperator()
    delattr(operator, "invocation_mode")
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["invocation_mode"] is None


def test_build_task_metrics_handles_custom_invocation_mode_string():
    operator = DummyDbtOperator()
    operator.invocation_mode = "custom-mode"
    ti = _make_task_instance(operator)

    metrics = task_instance_listener._build_task_metrics(ti, status="success")

    assert metrics["invocation_mode"] == "custom-mode"


def test_has_callback_returns_false_for_non_cosmos_task():
    ti = _make_task_instance(NonCosmosOperator())

    assert task_instance_listener._has_callback(ti) is False


def test_install_deps_returns_none_for_non_cosmos_task():
    ti = _make_task_instance(NonCosmosOperator())

    assert task_instance_listener._install_deps(ti) is None


def test_dbt_command_returns_none_for_non_cosmos_task():
    ti = _make_task_instance(NonCosmosOperator())

    assert task_instance_listener._dbt_command(ti) is None


@patch("cosmos.listeners.task_instance_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_task_instance_success_emits_for_cosmos_task(mock_emit):
    operator = DummyDbtOperator()
    ti = _make_task_instance(operator)

    task_instance_listener.on_task_instance_success(None, ti, session=None)

    mock_emit.assert_called_once()
    args, _ = mock_emit.call_args
    assert args[0] == task_instance_listener.TASK_INSTANCE_EVENT
    assert args[1]["status"] == "success"
    assert args[1]["dbt_command"] == "run"


@patch("cosmos.listeners.task_instance_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_task_instance_failed_emits_failed_status(mock_emit):
    operator = DummyDbtOperator()
    ti = _make_task_instance(operator)

    task_instance_listener.on_task_instance_failed(None, ti, error=None, session=None)

    mock_emit.assert_called_once()
    args, _ = mock_emit.call_args
    assert args[0] == task_instance_listener.TASK_INSTANCE_EVENT
    assert args[1]["status"] == "failed"


@patch("cosmos.listeners.task_instance_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_task_instance_success_skips_non_cosmos_task(mock_emit):
    ti = _make_task_instance(NonCosmosOperator())

    task_instance_listener.on_task_instance_success(None, ti, session=None)

    mock_emit.assert_not_called()


@patch("cosmos.listeners.task_instance_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_task_instance_failed_skips_non_cosmos_task(mock_emit):
    ti = _make_task_instance(NonCosmosOperator())

    task_instance_listener.on_task_instance_failed(None, ti, error=None, session=None)

    mock_emit.assert_not_called()
