from unittest.mock import MagicMock, patch

import pytest

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator  # noqa: F401
except ImportError:
    pytest.skip("Google Cloud provider not installed", allow_module_level=True)

from cosmos.config import ProfileConfig
from cosmos.operators.gcp_gke import (
    DbtBuildGcpGkeOperator,
    DbtCloneGcpGkeOperator,
    DbtLSGcpGkeOperator,
    DbtRunGcpGkeOperator,
    DbtRunOperationGcpGkeOperator,
    DbtSeedGcpGkeOperator,
    DbtSnapshotGcpGkeOperator,
    DbtSourceGcpGkeOperator,
    DbtTestGcpGkeOperator,
    DbtTestWarningHandlerGcpGke,
)

GKE_KWARGS = {
    "project_id": "my-gcp-project",
    "location": "us-central1",
    "cluster_name": "my-gke-cluster",
}

base_kwargs = {
    **GKE_KWARGS,
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def test_dbt_gcp_gke_build_command():
    """
    Since we know that the GKEStartPodOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """
    result_map = {
        "ls": DbtLSGcpGkeOperator(**base_kwargs),
        "run": DbtRunGcpGkeOperator(**base_kwargs),
        "test": DbtTestGcpGkeOperator(**base_kwargs),
        "build": DbtBuildGcpGkeOperator(**base_kwargs),
        "seed": DbtSeedGcpGkeOperator(**base_kwargs),
        "clone": DbtCloneGcpGkeOperator(**base_kwargs),
    }

    for command_name, command_operator in result_map.items():
        command_operator.build_kube_args(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.cmds == ["dbt"]
        assert command_operator.arguments == [
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
            "--project-dir",
            "my/dir",
        ]


@patch("cosmos.operators.gcp_gke.DbtGcpGkeBaseOperator.build_kube_args")
@patch("cosmos.operators.gcp_gke.GKEStartPodOperator.execute")
def test_dbt_gcp_gke_operator_execute(mock_gke_execute, mock_build_kube_args):
    """Tests that the execute method call results in both the build_kube_args method and the GKE execute method being called."""
    operator = DbtLSGcpGkeOperator(
        project_id="my-gcp-project",
        location="us-central1",
        cluster_name="my-gke-cluster",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    # Assert that the build_kube_args method was called in the execution
    mock_build_kube_args.assert_called_once()

    # Assert that the GKE execute method was called in the execution
    mock_gke_execute.assert_called_once()


def test_all_operator_subclasses_instantiate():
    """All GCP GKE operator subclasses can be instantiated without errors."""
    operator_classes = [
        (DbtBuildGcpGkeOperator, {}),
        (DbtLSGcpGkeOperator, {}),
        (DbtRunGcpGkeOperator, {}),
        (DbtSeedGcpGkeOperator, {}),
        (DbtSnapshotGcpGkeOperator, {}),
        (DbtTestGcpGkeOperator, {}),
        (DbtSourceGcpGkeOperator, {}),
        (DbtCloneGcpGkeOperator, {}),
        (DbtRunOperationGcpGkeOperator, {"macro_name": "my_macro"}),
    ]
    for cls, extra_kwargs in operator_classes:
        op = cls(**base_kwargs, **extra_kwargs)
        assert op.project_dir == "my/dir"


def test_build_kube_args_with_profile_config(tmp_path):
    """build_kube_args includes --profile and --target when profile_config is set."""
    profiles_yml = tmp_path / "profiles.yml"
    profiles_yml.write_text("my_profile:")
    profile_config = ProfileConfig(
        profile_name="my_profile",
        target_name="prod",
        profiles_yml_filepath=profiles_yml,
    )
    op = DbtRunGcpGkeOperator(
        **base_kwargs,
        profile_config=profile_config,
    )
    op.build_kube_args(context=MagicMock(), cmd_flags=None)

    assert "--profile" in op.arguments
    assert "my_profile" in op.arguments
    assert "--target" in op.arguments
    assert "prod" in op.arguments


def test_build_kube_args_without_profile_config():
    """build_kube_args omits --profile and --target when profile_config is None."""
    op = DbtRunGcpGkeOperator(**base_kwargs)
    op.build_kube_args(context=MagicMock(), cmd_flags=None)

    assert "--profile" not in op.arguments
    assert "--target" not in op.arguments


def test_build_env_args_merges_env_and_existing_env_vars():
    """build_env_args merges incoming env dict with operator's existing env_vars."""
    op = DbtRunGcpGkeOperator(**base_kwargs)
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars

    op.env_vars = convert_env_vars({"EXISTING_KEY": "existing_value"})
    op.build_env_args({"NEW_KEY": "new_value"})

    env_names = {ev.name for ev in op.env_vars}
    assert "NEW_KEY" in env_names
    assert "EXISTING_KEY" in env_names


def test_container_resources_dict_converted():
    """When container_resources is a dict, it is converted to V1ResourceRequirements."""
    import kubernetes.client as k8s

    kwargs = {
        **base_kwargs,
        "container_resources": {"requests": {"cpu": "100m", "memory": "256Mi"}},
    }
    op = DbtRunGcpGkeOperator(**kwargs)
    assert isinstance(op.container_resources, k8s.V1ResourceRequirements)


# --- DbtTestWarningHandlerGcpGke tests ---


def test_detect_standard_warnings_found():
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    log_text = "10:29:03  Done. PASS=5 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8"
    assert handler._detect_standard_warnings(log_text) == 3


def test_detect_standard_warnings_zero():
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    log_text = "10:29:03  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=5"
    assert handler._detect_standard_warnings(log_text) == 0


def test_detect_standard_warnings_not_found():
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    assert handler._detect_standard_warnings("some unrelated log output") is None


def test_detect_source_freshness_warnings_detailed():
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    log_text = "10:30:00  1 of 2  WARN freshness of source.my_db.my_table  [WARN in 3.5s]"
    result = handler._detect_source_freshness_warnings(log_text)
    assert len(result) == 1
    assert result[0]["source"] == "source.my_db.my_table"
    assert result[0]["timestamp"] == "10:30:00"
    assert result[0]["execution_time"] == "3.5"
    assert result[0]["status"] == "WARN"
    assert result[0]["type"] == "source_freshness"


def test_detect_source_freshness_warnings_simple_fallback():
    """Simple pattern is used for lines that don't match the detailed pattern."""
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    log_text = "WARN freshness of source.db.simple_table"
    result = handler._detect_source_freshness_warnings(log_text)
    assert len(result) == 1
    assert result[0]["source"] == "source.db.simple_table"
    assert "timestamp" not in result[0]


def test_detect_source_freshness_warnings_no_duplicates():
    """A source matched by the detailed pattern should not be duplicated by the simple pattern."""
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    log_text = "10:30:00  1 of 1  WARN freshness of source.db.tbl  [WARN in 2.0s]"
    result = handler._detect_source_freshness_warnings(log_text)
    assert len(result) == 1


def test_detect_source_freshness_warnings_empty():
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=MagicMock())
    assert handler._detect_source_freshness_warnings("no warnings here") == []


def test_on_pod_completion_no_context_logs_warning():
    """on_pod_completion logs a warning and returns early when context is None."""
    operator = MagicMock()
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=operator, context=None)
    handler.on_pod_completion(pod=MagicMock())
    operator.log.warning.assert_called_once_with("No context provided to the DbtTestWarningHandlerGcpGke.")


def test_on_pod_completion_wrong_task_type_logs_warning():
    """on_pod_completion logs a warning when the task type is not a test or source operator."""
    operator = MagicMock()
    task = MagicMock()  # not a DbtTestGcpGkeOperator or DbtSourceGcpGkeOperator
    context = {"task_instance": MagicMock(task=task)}
    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=MagicMock(), operator=operator, context=context)
    handler.on_pod_completion(pod=MagicMock())
    operator.log.warning.assert_called_once()
    assert "Cannot handle dbt warnings" in str(operator.log.warning.call_args)


def test_on_pod_completion_test_operator_with_warnings():
    """on_pod_completion detects warnings from DbtTestGcpGkeOperator and calls the callback."""
    callback = MagicMock()
    operator = MagicMock()
    task = MagicMock(spec=DbtTestGcpGkeOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Running 3 tests",
        b"10:29:03  Done. PASS=2 WARN=1 ERROR=0 SKIP=0 TOTAL=3",
    ]
    context = {"task_instance": MagicMock(task=task)}

    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=callback, operator=operator, context=context)

    with patch("cosmos.operators.gcp_gke.extract_log_issues", return_value=(["test1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once()


def test_on_pod_completion_source_operator_with_freshness_warnings():
    """on_pod_completion detects freshness warnings from DbtSourceGcpGkeOperator and calls the callback."""
    callback = MagicMock()
    operator = MagicMock()
    task = MagicMock(spec=DbtSourceGcpGkeOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"WARN freshness of source.db.stale_table",
    ]
    context = {"task_instance": MagicMock(task=task)}

    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=callback, operator=operator, context=context)

    with patch("cosmos.operators.gcp_gke.extract_log_issues", return_value=(["src1"], ["warn"])):
        handler.on_pod_completion(pod=MagicMock())

    callback.assert_called_once()


def test_on_pod_completion_no_warnings_logs_failure():
    """on_pod_completion logs a warning when no warning patterns are detected."""
    callback = MagicMock()
    operator = MagicMock()
    task = MagicMock(spec=DbtTestGcpGkeOperator)
    task.pod_manager.read_pod_logs.return_value = [
        b"10:29:03  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5",
    ]
    context = {"task_instance": MagicMock(task=task)}

    handler = DbtTestWarningHandlerGcpGke(on_warning_callback=callback, operator=operator, context=context)
    handler.on_pod_completion(pod=MagicMock())

    callback.assert_not_called()
    operator.log.warning.assert_called_once()
    assert "Failed to scrape warning count" in str(operator.log.warning.call_args)


# --- DbtWarningGcpGkeOperator tests ---


def test_warning_operator_with_callback_sets_handler():
    """DbtWarningGcpGkeOperator sets up warning_handler when on_warning_callback is provided."""
    callback = MagicMock()
    op = DbtTestGcpGkeOperator(**base_kwargs, on_warning_callback=callback)
    assert op.warning_handler is not None
    assert isinstance(op.warning_handler, DbtTestWarningHandlerGcpGke)


def test_warning_operator_without_callback_no_handler():
    """DbtWarningGcpGkeOperator has no handler when on_warning_callback is None."""
    op = DbtTestGcpGkeOperator(**base_kwargs)
    assert op.warning_handler is None


@patch("cosmos.operators.gcp_gke.DbtGcpGkeBaseOperator.build_and_run_cmd")
def test_warning_operator_build_and_run_cmd_sets_context(mock_super_build):
    """build_and_run_cmd injects context into warning_handler before calling super."""
    callback = MagicMock()
    op = DbtTestGcpGkeOperator(**base_kwargs, on_warning_callback=callback)
    ctx = MagicMock()

    op.build_and_run_cmd(context=ctx)

    assert op.warning_handler.context is ctx
    mock_super_build.assert_called_once()
