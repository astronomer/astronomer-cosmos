"""Tests for the shared K8s operator helpers in cosmos.operators._k8s_common."""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cosmos.constants import _PRODUCER_CMD_FLAGS_XCOM_KEY
from cosmos.operators._k8s_common import (
    CONTEXT_HOLDER_KEY,
    CONTEXT_KEY,
    DbtTestWarningHandler,
    WatcherK8sCallback,
    _build_env_vars,
    compose_watcher_backup_callbacks,
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


def test_build_env_vars_merges_env_and_existing_dict():
    """cncf-kubernetes >= 10.23.0 leaves ``operator.env_vars`` as a raw dict after construction."""
    result = _build_env_vars({"NEW_KEY": "new_value"}, {"EXISTING_KEY": "existing_value"})

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
        pytest.param("10:29:03  Summary: 4 total | 1 success | 1 warn | 2 error", 1, id="dbt_v2_found"),
        pytest.param("10:29:03  Summary: 2 total | 2 success", None, id="dbt_v2_no_warn"),
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


def test_on_pod_completion_dbt_v2_logs_fire_the_callback_with_the_warned_tests():
    callback = MagicMock()
    task = MagicMock(spec=DbtTestKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Warned test  accepted_values_orders_status (models/schema.yml) [1 of 3 in 0.5s]",
        b"10:29:03  Summary: 3 total | 2 success | 1 warn",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, context=context)

    handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once_with(context)
    assert context["test_names"] == ["accepted_values_orders_status"]


def test_on_pod_completion_falls_back_to_provider_context():
    """After a deferral, the resumed operator never ran ``execute``: the handler uses the context from the provider."""
    callback = MagicMock()
    task = MagicMock(spec=DbtTestKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Done. PASS=2 WARN=1 ERROR=0 SKIP=0 TOTAL=3",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, context=None)

    with patch("cosmos.operators._k8s_common.extract_log_issues", return_value=(["test1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock(), context=context, operator=task)

    callback.assert_called_once_with(context)


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


# Real output of ``dbt source freshness`` on dbt 2 (dbt OSS) for a source that is stale beyond ``warn_after``.
DBT_V2_FRESHNESS_WARN_LOG = """\
dbt-oss 2.0.8
Loading profiles.yml
Loading packages.yml
Warned source raw.raw_orders (last updated 8years 6months 20days 4h 43m 20s ago) [5.73s]
Successfully wrote sources.json
=================== Errors and Warnings ====================
[warning] [StaleSource (dbt1063)]: Stale source source.jaffle_shop.raw.raw_orders
--> models/staging/sources.yml
==================== Execution Summary ====================
Finished 'freshness' with 1 warning for target 'dev' [6.0s]
"""


def test_detect_source_freshness_warnings_dbt_v2():
    """dbt 2 prints ``Warned source <name> (...) [Ns]`` instead of ``WARN freshness of``."""
    handler = _make_handler()
    result = handler._detect_source_freshness_warnings(DBT_V2_FRESHNESS_WARN_LOG)
    assert result == [
        {
            "name": "source_freshness_raw.raw_orders",
            "status": "WARN",
            "type": "source_freshness",
            "source": "raw.raw_orders",
            "execution_time": "5.73",
        }
    ]


@pytest.mark.parametrize(
    ("log_text", "expected_sources"),
    (
        pytest.param(
            "20:18:55  Warned source raw.raw_orders (last updated 1h ago) [1.2s]", ["raw.raw_orders"], id="timestamp"
        ),
        pytest.param(
            "Warned source raw.a (last updated 1h ago) [1.0s]\nWarned source raw.b (last updated 2h ago) [2.0s]",
            ["raw.a", "raw.b"],
            id="several_sources",
        ),
        pytest.param(
            "Warned source raw.a (last updated 1h ago) [1.0s]\nWarned source raw.a (last updated 1h ago) [1.0s]",
            ["raw.a"],
            id="same_source_once",
        ),
        pytest.param("Warned source raw.a", ["raw.a"], id="without_details"),
        pytest.param("Stale source raw.orders_stale (last updated 1y ago) [5.8s]", [], id="error_is_not_a_warning"),
        pytest.param(
            "Warned test unique_orders_id (models/schema.yml:12:22) [3 of 8 in 3.4s]",
            [],
            id="warned_test_is_not_a_source",
        ),
        pytest.param("Source raw.orders is fresh [1.0s]", [], id="fresh_source"),
    ),
)
def test_detect_source_freshness_warnings_dbt_v2_lines(log_text, expected_sources):
    handler = _make_handler()
    result = handler._detect_source_freshness_warnings(log_text)
    assert [w["source"] for w in result] == expected_sources


def test_detect_source_freshness_warnings_dbt_v2_without_details_has_no_execution_time():
    handler = _make_handler()
    (warning,) = handler._detect_source_freshness_warnings("Warned source raw.a")
    assert "execution_time" not in warning


def test_on_pod_completion_source_operator_with_dbt_v2_freshness_warning():
    callback = MagicMock()
    task = MagicMock(spec=DbtSourceKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [line.encode() for line in DBT_V2_FRESHNESS_WARN_LOG.splitlines()]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, context=context)

    with patch("cosmos.operators._k8s_common.extract_log_issues", return_value=(["src1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once()


def test_on_pod_completion_source_operator_dbt_v2_fresh_source_does_not_call_callback():
    callback = MagicMock()
    operator = MagicMock()
    task = MagicMock(spec=DbtSourceKubernetesOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"Finished 'freshness' successfully for target 'dev' [6.0s]",
    ]
    context = {"task_instance": MagicMock(task=task)}
    handler = _make_handler(on_warning_callback=callback, operator=operator, context=context)

    handler.on_pod_completion(pod=MagicMock())

    callback.assert_not_called()


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
        pytest.param([], [WatcherK8sCallback], id="empty_list"),
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


def test_progress_callback_delegates_with_correct_args():
    """progress_callback unwraps the context holder and forwards test maps from kwargs.

    ``CosmosKubernetesPodManager`` threads these through ``callback_extra_kwargs``;
    they arrive in ``progress_callback`` as ``**kwargs`` (see #2543).
    """
    mock_context = {"ti": MagicMock()}
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    test_results: dict[str, dict[str, str]] = {}
    line = _make_dbt_log_line()

    with patch("cosmos.operators._k8s_common.store_dbt_resource_status_from_log") as mock_store:
        WatcherK8sCallback.progress_callback(
            line=line,
            **_CALLBACK_KWARGS,
            context_holder={CONTEXT_KEY: mock_context},
            tests_per_model=tests_per_model,
            test_results_per_model=test_results,
        )

    mock_store.assert_called_once()
    args, call_kwargs = mock_store.call_args
    assert args[0] == line
    assert args[1] == {"context": mock_context}  # allowlist: nothing else leaks past the unwrap
    assert args[1]["context"] is mock_context
    assert call_kwargs["tests_per_model"] is tests_per_model
    assert call_kwargs["test_results_per_model"] is test_results
    assert call_kwargs["upstream_failure_skipped_ids"] is None


def test_progress_callback_marks_upstream_failure_skip_in_status_xcom():
    """#3023: a pod's info-level dbt log has no ``NodeFinished``, so ``SkippingDetails`` alone marks the status XCom."""
    ti = MagicMock(spec=["xcom_push"])

    WatcherK8sCallback.progress_callback(
        line=_make_dbt_log_line("model.pkg.downstream", "skipped", "SkippingDetails"),
        **_CALLBACK_KWARGS,
        context_holder={CONTEXT_KEY: {"ti": ti}},
        tests_per_model={},
        test_results_per_model={},
        upstream_failure_skipped_ids=set(),
    )

    # The only XCom written for the node is its status.
    ti.xcom_push.assert_called_once_with(
        key="model__pkg__downstream_status",
        value={"status": "failed", "outlet_uris": [], "upstream_failure_skip": True},
    )


def test_callback_opts_in_to_cosmos_callback_kwargs():
    """The marker attribute read by CosmosKubernetesPodManager._extra_kwargs_for must be set."""
    assert WatcherK8sCallback.receives_cosmos_callback_kwargs is True


# ---------------------------------------------------------------------------
# build_watcher_pod_manager
# ---------------------------------------------------------------------------


def test_build_watcher_pod_manager_wires_callback_extra_kwargs():
    """The pod manager receives the producer's per-execution state by reference."""
    from cosmos.operators._k8s_common import build_watcher_pod_manager

    operator = MagicMock()
    operator._tests_per_model = {"model.pkg.orders": ["test.pkg.t1"]}
    operator._test_results_per_model = {}
    operator._context_holder = {CONTEXT_KEY: {"ti": MagicMock()}}
    operator._upstream_failure_skipped_ids = set()
    operator._model_outlet_uris = {}
    operator._should_generate_model_uris = True

    with patch("cosmos.operators._k8s_common.CosmosKubernetesPodManager") as mock_manager_cls:
        build_watcher_pod_manager(operator)

    mock_manager_cls.assert_called_once_with(
        kube_client=operator.client,
        callbacks=operator.callbacks,
        callback_extra_kwargs={
            "tests_per_model": operator._tests_per_model,
            "test_results_per_model": operator._test_results_per_model,
            CONTEXT_HOLDER_KEY: operator._context_holder,
            "upstream_failure_skipped_ids": operator._upstream_failure_skipped_ids,
            "model_outlet_uris": operator._model_outlet_uris,
            "should_generate_model_uris": operator._should_generate_model_uris,
        },
    )


def test_pod_manager_passes_extra_kwargs_only_to_marked_callbacks():
    """callback_extra_kwargs reach WatcherK8sCallback but not unmarked user callbacks (#2543)."""
    from cosmos.airflow._override import CosmosKubernetesPodManager

    extra = {
        "tests_per_model": {"m": ["t"]},
        "test_results_per_model": {},
        CONTEXT_HOLDER_KEY: {CONTEXT_KEY: {"ti": MagicMock()}},
    }
    manager = CosmosKubernetesPodManager(
        kube_client=MagicMock(),
        callbacks=[WatcherK8sCallback],
        callback_extra_kwargs=extra,
    )
    assert manager._callback_extra_kwargs is extra

    # WatcherK8sCallback opts in via the marker -> receives the kwargs (same object).
    assert manager._extra_kwargs_for(WatcherK8sCallback) is extra

    # A user-supplied callback without the marker receives nothing, so its progress_callback
    # (which may not accept Cosmos-only kwargs) is never passed them and cannot raise TypeError.
    class UserCallback:
        @staticmethod
        def progress_callback(*, line, client, mode, container_name, timestamp, pod): ...

    assert manager._extra_kwargs_for(UserCallback) == {}

    # Default when omitted is an empty dict so the spread is always safe.
    bare_manager = CosmosKubernetesPodManager(kube_client=MagicMock())
    assert bare_manager._callback_extra_kwargs == {}


# ---------------------------------------------------------------------------
# execute_watcher_producer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("extra_kwargs",),
    (
        pytest.param({}, id="first_attempt"),
        pytest.param({"extra_arg": "value"}, id="forwards_kwargs"),
    ),
)
@patch("cosmos.settings.enable_watcher_reliable_retry", True)
@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer(mock_init, mock_delete, extra_kwargs: dict):
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}
    parent_execute = MagicMock(return_value="result")

    result = execute_watcher_producer(MagicMock(), context, parent_execute, **extra_kwargs)

    assert result == "result"
    parent_execute.assert_called_once_with(context, **extra_kwargs)
    mock_init.assert_called_once_with(context, persist=True)
    mock_delete.assert_called_once_with(context)


@pytest.mark.parametrize("reliable_retry", [True, False])
@patch("cosmos.operators._k8s_common._restore_xcom_backup")
def test_execute_watcher_producer_skips_retry(mock_restore, reliable_retry):
    """A retry restores and skips; the task store is read only when enable_watcher_reliable_retry is on (#2771)."""
    from airflow.exceptions import AirflowSkipException

    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti, "run_id": "test_run"}
    parent_execute = MagicMock()

    with (
        patch("cosmos.settings.enable_watcher_reliable_retry", reliable_retry),
        pytest.raises(AirflowSkipException, match="does not support Airflow retries"),
    ):
        execute_watcher_producer(MagicMock(), context, parent_execute)

    mock_restore.assert_called_once_with(context, read_task_state_store=reliable_retry)
    parent_execute.assert_not_called()


@patch("cosmos.settings.enable_watcher_reliable_retry", True)
@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer_keeps_backup_on_failure(mock_init, mock_delete):
    """On failure the backup Variable is kept (not deleted) for the retry; the on-failure
    callback (see ``compose_watcher_backup_callbacks``) performs the flush, not ``execute``."""
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}
    parent_execute = MagicMock(side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        execute_watcher_producer(MagicMock(), context, parent_execute)

    mock_init.assert_called_once_with(context, persist=True)
    mock_delete.assert_not_called()


@patch("cosmos.settings.enable_watcher_reliable_retry", False)
@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer_in_memory_mode_skips_variable_backup(mock_init, mock_delete):
    """With enable_watcher_reliable_retry=False the producer keeps the buffer in memory only (#2776)."""
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}
    parent_execute = MagicMock(return_value="result")

    result = execute_watcher_producer(MagicMock(), context, parent_execute)

    assert result == "result"
    mock_init.assert_called_once_with(context, persist=False)
    mock_delete.assert_not_called()


@patch("cosmos.settings.enable_watcher_reliable_retry", False)
@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer_not_reliable_keeps_the_variable_flush_with_a_task_state_store(mock_init, mock_delete):
    """With enable_watcher_reliable_retry=False the task store is left alone and nothing is deleted on success (#2771)."""
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run", "task_state_store": MagicMock()}

    execute_watcher_producer(MagicMock(), context, MagicMock(return_value="result"))

    mock_init.assert_called_once_with(context, persist=False)
    mock_delete.assert_not_called()


@pytest.mark.parametrize(
    "task_state_store,expected_level",
    [(MagicMock(), logging.WARNING), (None, logging.INFO)],
    ids=["task-store-in-use", "variable-only"],
)
@patch("cosmos.settings.enable_watcher_reliable_retry", True)
@patch("cosmos.operators._k8s_common._restore_xcom_backup", return_value=False)
def test_execute_watcher_producer_logs_when_no_backup_is_found_on_retry(
    mock_restore, caplog, task_state_store, expected_level
):
    from airflow.exceptions import AirflowSkipException

    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti, "run_id": "test_run"}
    if task_state_store is not None:
        context["task_state_store"] = task_state_store

    with caplog.at_level(logging.INFO), pytest.raises(AirflowSkipException):
        execute_watcher_producer(MagicMock(), context, MagicMock())

    record = next(r for r in caplog.records if "No XCom backup found for attempt #2" in r.getMessage())
    assert record.levelno == expected_level


def test_execute_watcher_producer_raises_when_ti_missing():
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="expects a task instance"):
        execute_watcher_producer(MagicMock(), {"ti": None}, MagicMock())


@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer_sets_context_before_parent_execute(mock_init, mock_delete):
    """The context holder (read by build_watcher_pod_manager) is populated before parent_execute runs."""
    operator = MagicMock()
    operator._context_holder = {CONTEXT_KEY: None}
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}
    captured: dict[str, Any] = {}

    def capture_context(ctx, **kwargs):
        captured["context"] = operator._context_holder[CONTEXT_KEY]

    execute_watcher_producer(operator, context, capture_context)

    assert captured["context"] is context
    operator._upstream_failure_skipped_ids.clear.assert_called_once_with()


@patch("cosmos.operators._k8s_common._delete_xcom_backup")
@patch("cosmos.operators._k8s_common._init_xcom_backup")
def test_execute_watcher_producer_publishes_own_cmd_flags_to_xcom(mock_init, mock_delete):
    """The producer must publish its own rendered ``add_cmd_flags()`` so consumer fallback runs can
    reuse it instead of re-deriving flags from a possibly different consumer operator_args.
    """
    operator = MagicMock()
    operator.add_cmd_flags.return_value = ["--full-refresh"]
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}

    execute_watcher_producer(operator, context, MagicMock(return_value="result"))

    ti.xcom_push.assert_any_call(key=_PRODUCER_CMD_FLAGS_XCOM_KEY, value=["--full-refresh"])


# ---------------------------------------------------------------------------
# compose_watcher_backup_callbacks
# ---------------------------------------------------------------------------


def test_compose_watcher_backup_callbacks_registers_on_both_callbacks():
    """The XCom backup flush is appended to both on_retry_callback and on_failure_callback (#2776)."""
    from cosmos.operators._watcher.xcom import _backup_xcom_to_variable

    operator = MagicMock()
    operator.on_retry_callback = None
    operator.on_failure_callback = None

    compose_watcher_backup_callbacks(operator)

    for cb in (operator.on_retry_callback, operator.on_failure_callback):
        callbacks = list(cb) if isinstance(cb, (list, tuple)) else [cb]
        assert _backup_xcom_to_variable in callbacks


def test_compose_watcher_backup_callbacks_preserves_existing():
    """An existing default_args callback is preserved when the backup flush is appended (#2776)."""
    from cosmos.operators._watcher.xcom import _backup_xcom_to_variable

    existing = MagicMock()
    operator = MagicMock()
    operator.on_retry_callback = existing
    operator.on_failure_callback = [existing]

    compose_watcher_backup_callbacks(operator)

    assert existing in operator.on_retry_callback
    assert _backup_xcom_to_variable in operator.on_retry_callback
    assert existing in operator.on_failure_callback
    assert _backup_xcom_to_variable in operator.on_failure_callback
