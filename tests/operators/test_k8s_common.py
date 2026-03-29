"""Tests for the shared K8s operator helpers in cosmos.operators._k8s_common."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cosmos.operators._k8s_common import (
    DbtTestWarningHandler,
    WatcherK8sCallback,
    _build_env_vars,
    execute_watcher_producer,
    inject_watcher_callback,
)
from cosmos.operators.kubernetes import (
    DbtRunKubernetesOperator,
    DbtSourceKubernetesOperator,
    DbtTestKubernetesOperator,
)

base_kwargs = {
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "no_version_check": True,
}


def _make_handler(**overrides: Any) -> DbtTestWarningHandler:
    """Create a DbtTestWarningHandler with sensible defaults."""
    defaults: dict[str, Any] = {
        "on_warning_callback": MagicMock(),
        "operator": MagicMock(),
        "test_operator_class": DbtTestKubernetesOperator,
        "source_operator_class": DbtSourceKubernetesOperator,
    }
    defaults.update(overrides)
    return DbtTestWarningHandler(**defaults)


# ---------------------------------------------------------------------------
# _build_env_vars
# ---------------------------------------------------------------------------


def test_build_env_vars_merges_env_and_existing():
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars

    existing = convert_env_vars({"EXISTING_KEY": "existing_value"})
    result = _build_env_vars({"NEW_KEY": "new_value"}, existing)

    env_names = {ev.name for ev in result}
    assert "NEW_KEY" in env_names
    assert "EXISTING_KEY" in env_names


# ---------------------------------------------------------------------------
# build_kube_args
# ---------------------------------------------------------------------------


def test_build_kube_args_with_profile_config(tmp_path):
    from cosmos.config import ProfileConfig

    profiles_yml = tmp_path / "profiles.yml"
    profiles_yml.write_text("my_profile:")
    profile_config = ProfileConfig(
        profile_name="my_profile",
        target_name="prod",
        profiles_yml_filepath=profiles_yml,
    )
    op = DbtRunKubernetesOperator(**base_kwargs, profile_config=profile_config)
    op.build_kube_args(context=MagicMock(), cmd_flags=None)

    assert "--profile" in op.arguments
    assert "my_profile" in op.arguments
    assert "--target" in op.arguments
    assert "prod" in op.arguments


def test_build_kube_args_without_profile_config():
    op = DbtRunKubernetesOperator(**base_kwargs)
    op.build_kube_args(context=MagicMock(), cmd_flags=None)

    assert "--profile" not in op.arguments
    assert "--target" not in op.arguments


def test_container_resources_dict_converted():
    import kubernetes.client as k8s

    kwargs = {**base_kwargs, "container_resources": {"requests": {"cpu": "100m", "memory": "256Mi"}}}
    op = DbtRunKubernetesOperator(**kwargs)
    assert isinstance(op.container_resources, k8s.V1ResourceRequirements)


# ---------------------------------------------------------------------------
# DbtTestWarningHandler._detect_standard_warnings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("log_text", "expected"),
    (
        pytest.param("10:29:03  Done. PASS=5 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8", 3, id="found"),
        pytest.param("10:29:03  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=5", 0, id="zero"),
        pytest.param("some unrelated log output", None, id="not_found"),
    ),
)
def test_detect_standard_warnings(log_text: str, expected: int | None):
    handler = _make_handler()
    assert handler._detect_standard_warnings(log_text) == expected


# ---------------------------------------------------------------------------
# DbtTestWarningHandler._detect_source_freshness_warnings
# ---------------------------------------------------------------------------


def test_detect_source_freshness_warnings_detailed():
    """Detailed pattern extracts timestamp, source, and execution time."""
    handler = _make_handler()
    log_text = "10:30:00  1 of 2  WARN freshness of source.my_db.my_table  [WARN in 3.5s]"
    result = handler._detect_source_freshness_warnings(log_text)
    assert len(result) == 1
    assert result[0]["source"] == "source.my_db.my_table"
    assert result[0]["timestamp"] == "10:30:00"
    assert result[0]["execution_time"] == "3.5"
    assert result[0]["status"] == "WARN"
    assert result[0]["type"] == "source_freshness"


@pytest.mark.parametrize(
    ("log_text", "expected_count", "expected_source"),
    (
        pytest.param("WARN freshness of source.db.simple_table", 1, "source.db.simple_table", id="simple_fallback"),
        pytest.param(
            "10:30:00  1 of 1  WARN freshness of source.db.tbl  [WARN in 2.0s]", 1, "source.db.tbl", id="no_dupes"
        ),
        pytest.param("no warnings here", 0, None, id="empty"),
    ),
)
def test_detect_source_freshness_warnings(log_text: str, expected_count: int, expected_source: str | None):
    handler = _make_handler()
    result = handler._detect_source_freshness_warnings(log_text)
    assert len(result) == expected_count
    if expected_source:
        assert result[0]["source"] == expected_source


# ---------------------------------------------------------------------------
# DbtTestWarningHandler.on_pod_completion
# ---------------------------------------------------------------------------


def test_on_pod_completion_no_context_logs_warning():
    operator = MagicMock()
    handler = _make_handler(operator=operator, context=None)
    handler.on_pod_completion(pod=MagicMock())
    operator.log.warning.assert_called_once_with("No context provided to the DbtTestWarningHandler.")


def test_on_pod_completion_wrong_task_type_logs_warning():
    operator = MagicMock()
    task = MagicMock()  # not a test or source operator
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(operator=operator, context=context)
    handler.on_pod_completion(pod=MagicMock())
    operator.log.warning.assert_called_once()
    assert "Cannot handle dbt warnings" in str(operator.log.warning.call_args)


def test_on_pod_completion_test_operator_with_warnings():
    callback = MagicMock()
    task = MagicMock(spec=DbtTestKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Running 3 tests",
        b"10:29:03  Done. PASS=2 WARN=1 ERROR=0 SKIP=0 TOTAL=3",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, context=context)

    with patch("cosmos.operators._k8s_common.extract_log_issues", return_value=(["test1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once()


def test_on_pod_completion_source_operator_with_freshness_warnings():
    callback = MagicMock()
    task = MagicMock(spec=DbtSourceKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"WARN freshness of source.db.stale_table",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, context=context)

    with patch("cosmos.operators._k8s_common.extract_log_issues", return_value=(["src1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once()


def test_on_pod_completion_no_warnings_logs_failure():
    callback = MagicMock()
    operator = MagicMock()
    task = MagicMock(spec=DbtTestKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, operator=operator, context=context)
    handler.on_pod_completion(pod=MagicMock())

    callback.assert_not_called()
    operator.log.warning.assert_called_once()
    assert "Failed to scrape warning count" in str(operator.log.warning.call_args)


# ---------------------------------------------------------------------------
# setup_warning_handler (via DbtTestKubernetesOperator)
# ---------------------------------------------------------------------------


def test_warning_operator_with_callback_sets_handler():
    callback = MagicMock()
    op = DbtTestKubernetesOperator(**base_kwargs, on_warning_callback=callback)
    assert op.warning_handler is not None
    assert isinstance(op.warning_handler, DbtTestWarningHandler)


def test_warning_operator_without_callback_no_handler():
    op = DbtTestKubernetesOperator(**base_kwargs)
    assert op.warning_handler is None


@patch("cosmos.operators.kubernetes.DbtKubernetesBaseOperator.build_and_run_cmd")
def test_warning_operator_build_and_run_cmd_sets_context(mock_super_build):
    callback = MagicMock()
    op = DbtTestKubernetesOperator(**base_kwargs, on_warning_callback=callback)
    ctx = MagicMock()

    op.build_and_run_cmd(context=ctx)

    assert op.warning_handler.context is ctx
    mock_super_build.assert_called_once()


# ---------------------------------------------------------------------------
# inject_watcher_callback
# ---------------------------------------------------------------------------


class CustomCallback1:
    pass


class CustomCallback2:
    pass


@pytest.mark.parametrize(
    ("callbacks", "expected"),
    (
        pytest.param(None, [WatcherK8sCallback], id="none"),
        pytest.param([CustomCallback1], [CustomCallback1, WatcherK8sCallback], id="list"),
        pytest.param((CustomCallback1,), [CustomCallback1, WatcherK8sCallback], id="tuple"),
        pytest.param(CustomCallback1, [CustomCallback1, WatcherK8sCallback], id="single"),
        pytest.param(
            [CustomCallback1, CustomCallback2],
            [CustomCallback1, CustomCallback2, WatcherK8sCallback],
            id="multiple",
        ),
        pytest.param((), [WatcherK8sCallback], id="empty_list"),
        pytest.param((), [WatcherK8sCallback], id="empty_tuple"),
    ),
)
def test_inject_watcher_callback(callbacks: list[Any] | tuple[Any] | None, expected: list[Any]) -> None:
    kwargs: dict[str, Any] = {"callbacks": callbacks}
    inject_watcher_callback(kwargs)
    assert kwargs["callbacks"] == expected


def test_inject_watcher_callback_no_callbacks_key():
    kwargs: dict[str, Any] = {}
    inject_watcher_callback(kwargs)
    assert kwargs["callbacks"] == [WatcherK8sCallback]


# ---------------------------------------------------------------------------
# WatcherK8sCallback.progress_callback
# ---------------------------------------------------------------------------


def _make_dbt_log_line(
    unique_id: str = "model.pkg.my_model",
    status: str = "success",
    event_name: str = "NodeFinished",
    resource_type: str = "model",
) -> str:
    return json.dumps(
        {
            "info": {"name": event_name, "level": "INFO", "msg": f"Node {unique_id} finished", "ts": ""},
            "data": {
                "node_info": {
                    "unique_id": unique_id,
                    "node_status": status,
                    "resource_type": resource_type,
                    "node_started_at": "",
                    "node_finished_at": "",
                }
            },
        }
    )


_CALLBACK_KWARGS = dict(client=MagicMock(), mode="sync", container_name="base", timestamp=None, pod=MagicMock())


def test_progress_callback_calls_store_dbt_resource_status():
    context = {"ti": MagicMock()}
    line = _make_dbt_log_line()

    with patch("cosmos.operators._k8s_common.store_dbt_resource_status_from_log") as mock_store:
        WatcherK8sCallback.progress_callback(line=line, **_CALLBACK_KWARGS, context=context)
        mock_store.assert_called_once_with(line, {"context": context})


def test_progress_callback_uses_global_context_when_not_in_kwargs():
    import cosmos.operators._k8s_common as mod

    fake_context = {"ti": MagicMock()}
    original = mod._producer_task_context
    try:
        mod._producer_task_context = fake_context

        with patch("cosmos.operators._k8s_common.store_dbt_resource_status_from_log") as mock_store:
            WatcherK8sCallback.progress_callback(line=_make_dbt_log_line(), **_CALLBACK_KWARGS)
            call_kwargs = mock_store.call_args[0][1]
            assert call_kwargs["context"] is fake_context
    finally:
        mod._producer_task_context = original


# ---------------------------------------------------------------------------
# execute_watcher_producer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("try_number", "extra_kwargs", "expect_call", "expected_result"),
    (
        pytest.param(1, {}, True, "result", id="first_attempt"),
        pytest.param(1, {"extra_arg": "value"}, True, "result", id="forwards_kwargs"),
        pytest.param(2, {}, False, None, id="skips_retry"),
    ),
)
def test_execute_watcher_producer(try_number: int, extra_kwargs: dict, expect_call: bool, expected_result: Any):
    ti = MagicMock()
    ti.try_number = try_number
    context = {"ti": ti}
    parent_execute = MagicMock(return_value="result")

    result = execute_watcher_producer(MagicMock(), context, parent_execute, **extra_kwargs)

    assert result == expected_result
    if expect_call:
        parent_execute.assert_called_once_with(context, **extra_kwargs)
    else:
        parent_execute.assert_not_called()


def test_execute_watcher_producer_raises_when_ti_missing():
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="expects a task instance"):
        execute_watcher_producer(MagicMock(), {"ti": None}, MagicMock())


def test_execute_watcher_producer_sets_global_context():
    import cosmos.operators._k8s_common as mod

    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti}
    captured: dict[str, Any] = {}

    def capture_context(ctx, **kwargs):
        captured["context"] = mod._producer_task_context

    original = mod._producer_task_context
    try:
        execute_watcher_producer(MagicMock(), context, capture_context)
        assert captured["context"] is context
    finally:
        mod._producer_task_context = original
