from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import kubernetes.client as k8s
import pytest
from airflow import __version__ as airflow_version
from airflow.models import DAG, TaskInstance
from airflow.utils.context import Context
from packaging import version
from pendulum import datetime

from cosmos import ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtCloneKubernetesOperator,
    DbtKubernetesBaseOperator,
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtSourceKubernetesOperator,
    DbtTestKubernetesOperator,
    DbtTestWarningHandler,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping


@pytest.fixture()
def mock_kubernetes_execute():
    with patch("cosmos.operators.kubernetes.KubernetesPodOperator.execute") as mock_execute:
        yield mock_execute


@pytest.fixture()
def fake_kubernetes_execute_pod_completion():
    def execute(self, *args, **kwargs):
        if isinstance(self.callbacks, list):
            for callback in self.callbacks:
                callback.on_pod_completion(pod="pod")
        else:
            self.callbacks.on_pod_completion(pod="pod")

    with patch("cosmos.operators.kubernetes.KubernetesPodOperator.execute", new=execute) as mock_execute:
        yield mock_execute


@pytest.fixture()
def base_operator(mock_kubernetes_execute):
    from cosmos.operators.kubernetes import DbtKubernetesBaseOperator

    class ConcreteDbtKubernetesBaseOperator(DbtKubernetesBaseOperator):
        base_cmd = ["cmd"]

    return ConcreteDbtKubernetesBaseOperator


@pytest.fixture
def kubernetes_pod_operator_callback():
    try:
        from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback

        return KubernetesPodOperatorCallback
    except ImportError:
        pytest.skip("Skipping: airflow.providers.cncf.kubernetes.callbacks not available")


def test_dbt_kubernetes_operator_add_global_flags(base_operator) -> None:
    dbt_kube_operator = base_operator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
    )
    assert dbt_kube_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@patch("cosmos.operators.kubernetes.DbtKubernetesBaseOperator.build_kube_args")
def test_dbt_kubernetes_operator_execute(mock_build_kube_args, base_operator, mock_kubernetes_execute):
    """Tests that the execute method call results in both the build_kube_args method and the kubernetes execute method being called."""
    operator = base_operator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    # Assert that the build_kube_args method was called in the execution
    mock_build_kube_args.assert_called_once()
    # Assert that the kubernetes execute method was called in the execution
    mock_kubernetes_execute.assert_called_once()
    assert mock_kubernetes_execute.call_args.args[-1] == {}


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_kubernetes_operator_get_env(p_context_to_airflow_vars: MagicMock, base_operator) -> None:
    """
    If an end user passes in a
    """
    dbt_kube_operator = base_operator(
        conn_id="my_airflow_connection", task_id="my-task", image="my_image", project_dir="my/dir", append_env=False
    )
    dbt_kube_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_kube_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
    }
    assert env == expected_env


base_kwargs = {
    "conn_id": "my_airflow_connection",
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}

result_map = {
    ("ls",): DbtLSKubernetesOperator(**base_kwargs),
    ("run",): DbtRunKubernetesOperator(**base_kwargs),
    ("test",): DbtTestKubernetesOperator(**base_kwargs),
    ("build",): DbtBuildKubernetesOperator(**base_kwargs),
    ("seed",): DbtSeedKubernetesOperator(**base_kwargs),
    ("clone",): DbtCloneKubernetesOperator(**base_kwargs),
    ("source", "freshness"): DbtSourceKubernetesOperator(**base_kwargs),
}


def test_dbt_kubernetes_build_command():
    """
    Since we know that the KubernetesOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_kube_args(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.arguments == [
            "dbt",
            *command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
            "--project-dir",
            "my/dir",
        ]


def test_dbt_test_kubernetes_operator_constructor(kubernetes_pod_operator_callback):
    test_operator = DbtTestKubernetesOperator(on_warning_callback=(lambda *args, **kwargs: None), **base_kwargs)
    if isinstance(test_operator.callbacks, list):
        assert any([isinstance(cb, kubernetes_pod_operator_callback) for cb in test_operator.callbacks])
    else:
        assert isinstance(test_operator.callbacks, kubernetes_pod_operator_callback)


def test_dbt_source_kubernetes_operator_constructor(kubernetes_pod_operator_callback):
    test_operator = DbtSourceKubernetesOperator(on_warning_callback=(lambda *args, **kwargs: None), **base_kwargs)
    if isinstance(test_operator.callbacks, list):
        assert any([isinstance(cb, kubernetes_pod_operator_callback) for cb in test_operator.callbacks])
    else:
        assert isinstance(test_operator.callbacks, kubernetes_pod_operator_callback)


class FakePodManager:
    def __init__(self, log_string):
        self.log_string = log_string

    def read_pod_logs(self, pod, container):
        assert pod == "pod"
        assert container == "base"
        return (log.encode("utf-8") for log in self.log_string.split("\n"))


def create_test_handler():
    """Helper function to create a test handler with mocks"""
    mock_callback = Mock()
    mock_operator = Mock()
    mock_context = {"task_instance": Mock()}
    handler = DbtTestWarningHandler(on_warning_callback=mock_callback, operator=mock_operator, context=mock_context)
    return handler, mock_callback, mock_operator, mock_context


@pytest.mark.parametrize(
    ("log_text", "expected_warn_count"),
    [
        # Standard warning with summary
        (
            """
            19:48:25  Concurrency: 4 threads (target='target')
            19:48:27  1 of 2 WARN dbt_utils_accepted_range ..................... [WARN 117 in 1.83s]
            19:48:27  2 of 2 PASS unique_table__uuid ................................................ [PASS in 1.85s]
            19:48:27  Done. PASS=1 WARN=1 ERROR=0 SKIP=0 TOTAL=2
            """,
            1,
        ),
        # Multiple warnings
        (
            """
            19:48:25  Concurrency: 4 threads (target='target')
            19:48:27  1 of 3 WARN test_one ..................... [WARN in 1.83s]
            19:48:27  2 of 3 WARN test_two ..................... [WARN in 1.85s]
            19:48:27  3 of 3 PASS test_three ................... [PASS in 1.85s]
            19:48:27  Done. PASS=1 WARN=2 ERROR=0 SKIP=0 TOTAL=3
            """,
            2,
        ),
        # No warnings
        (
            """
            19:48:25  Concurrency: 4 threads (target='target')
            19:48:27  1 of 2 PASS test_one ..................... [PASS in 1.83s]
            19:48:27  2 of 2 PASS test_two ..................... [PASS in 1.85s]
            19:48:27  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
            """,
            0,
        ),
        # No summary (like source freshness)
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:20 1 of 1 START freshness of auction_net.raw .......................... [RUN]
            15:49:21 1 of 1 WARN freshness of auction_net.raw ........................... [WARN in 0.90s]
            15:49:21 Done.
            """,
            None,
        ),
    ],
)
def test_detect_standard_warnings(log_text, expected_warn_count):
    """Test detection of standard dbt test warnings"""
    handler, _, _, _ = create_test_handler()
    warn_count = handler._detect_standard_warnings(log_text)
    assert warn_count == expected_warn_count


@pytest.mark.parametrize(
    ("log_text", "expected_warning_count", "expected_sources"),
    [
        # Single source freshness warning
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:20 1 of 1 START freshness of auction_net.auction_net_raw .......................... [RUN]
            15:49:21 1 of 1 WARN freshness of auction_net.auction_net_raw ........................... [WARN in 0.90s]
            15:49:21 Done.
            """,
            1,
            ["auction_net.auction_net_raw"],
        ),
        # Multiple source freshness warnings
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:20 1 of 3 START freshness of source1.table1 .......................... [RUN]
            15:49:21 1 of 3 WARN freshness of source1.table1 ........................... [WARN in 0.90s]
            15:49:21 2 of 3 START freshness of source2.table2 .......................... [RUN]
            15:49:22 2 of 3 WARN freshness of source2.table2 ........................... [WARN in 1.20s]
            15:49:22 3 of 3 START freshness of source3.table3 .......................... [RUN]
            15:49:23 3 of 3 PASS freshness of source3.table3 ........................... [PASS in 0.45s]
            15:49:23 Done.
            """,
            2,
            ["source1.table1", "source2.table2"],
        ),
        # No source freshness warnings - all pass
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:20 1 of 1 START freshness of auction_net.raw .......................... [RUN]
            15:49:21 1 of 1 PASS freshness of auction_net.raw ........................... [PASS in 0.90s]
            15:49:21 Done.
            """,
            0,
            [],
        ),
        # Empty source freshness log
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:21 Done.
            """,
            0,
            [],
        ),
    ],
)
def test_detect_source_freshness_warnings(log_text, expected_warning_count, expected_sources):
    """Test detection of source freshness warnings"""
    handler, _, _, _ = create_test_handler()
    warnings = handler._detect_source_freshness_warnings(log_text)
    assert len(warnings) == expected_warning_count

    if expected_sources:
        actual_sources = [w["source"] for w in warnings]
        for expected_source in expected_sources:
            assert expected_source in actual_sources


@pytest.mark.parametrize(
    ("log_text",),
    [
        # Source freshness log with single warning
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:18 Concurrency: 2 threads (target='default')
            15:49:20 1 of 1 START freshness of auction_net.auction_net_raw .......................... [RUN]
            15:49:21 1 of 1 WARN freshness of auction_net.auction_net_raw ........................... [WARN in 0.90s]
            15:49:21 Finished running 1 source in 0 hours 0 minutes and 3.27 seconds (3.27s).
            15:49:21 Done.
            """,
        ),
        # Source freshness log with multiple warnings
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:18 Concurrency: 2 threads (target='default')
            15:49:20 1 of 3 START freshness of source1.table1 .......................... [RUN]
            15:49:21 1 of 3 WARN freshness of source1.table1 ........................... [WARN in 0.90s]
            15:49:21 2 of 3 START freshness of source2.table2 .......................... [RUN]
            15:49:22 2 of 3 WARN freshness of source2.table2 ........................... [WARN in 1.20s]
            15:49:22 3 of 3 START freshness of source3.table3 .......................... [RUN]
            15:49:23 3 of 3 PASS freshness of source3.table3 ........................... [PASS in 0.45s]
            15:49:23 Finished running 3 sources in 0 hours 0 minutes and 5.12 seconds (5.12s).
            15:49:23 Done.
            """,
        ),
        # Source freshness log with no warnings
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:18 Concurrency: 2 threads (target='default')
            15:49:20 1 of 2 START freshness of auction_net.raw .......................... [RUN]
            15:49:21 1 of 2 PASS freshness of auction_net.raw ........................... [PASS in 0.90s]
            15:49:21 2 of 2 START freshness of another_source.table .......................... [RUN]
            15:49:22 2 of 2 PASS freshness of another_source.table ........................... [PASS in 0.45s]
            15:49:22 Finished running 2 sources in 0 hours 0 minutes and 4.32 seconds (4.32s).
            15:49:22 Done.
            """,
        ),
        # Source freshness log with mixed results
        (
            """
            15:49:18 Found 205 models, 27 data tests, 66 sources, 639 macros
            15:49:18 Concurrency: 2 threads (target='default')
            15:49:20 1 of 4 START freshness of source_a.table_a .......................... [RUN]
            15:49:21 1 of 4 PASS freshness of source_a.table_a ........................... [PASS in 0.90s]
            15:49:21 2 of 4 START freshness of source_b.table_b .......................... [RUN]
            15:49:22 2 of 4 WARN freshness of source_b.table_b ........................... [WARN in 1.10s]
            15:49:22 3 of 4 START freshness of source_c.table_c .......................... [RUN]
            15:49:23 3 of 4 PASS freshness of source_c.table_c ........................... [PASS in 0.45s]
            15:49:23 4 of 4 START freshness of source_d.table_d .......................... [RUN]
            15:49:24 4 of 4 WARN freshness of source_d.table_d ........................... [WARN in 0.78s]
            15:49:24 Finished running 4 sources in 0 hours 0 minutes and 6.45 seconds (6.45s).
            15:49:24 Done.
            """,
        ),
    ],
)
def test_source_freshness_log_formats(log_text):
    """Test various source freshness log formats to ensure parsing works correctly"""
    handler, _, _, _ = create_test_handler()
    warnings = handler._detect_source_freshness_warnings(log_text)

    # Count expected warnings by counting "WARN freshness of" occurrences
    expected_warnings = log_text.count("WARN freshness of")
    assert len(warnings) == expected_warnings

    # Verify each warning has required fields
    for warning in warnings:
        assert "name" in warning
        assert "status" in warning
        assert warning["status"] == "WARN"
        assert "type" in warning
        assert warning["type"] == "source_freshness"
        assert "source" in warning


@pytest.mark.parametrize(
    ("log_string", "should_call"),
    (
        (
            """
        19:48:25  Concurrency: 4 threads (target='target')
        19:48:25
        19:48:25  1 of 2 START test dbt_utils_accepted_range_table_col__12__0 ................... [RUN]
        19:48:25  2 of 2 START test unique_table__uuid .......................................... [RUN]
        19:48:27  1 of 2 WARN dbt_utils_accepted_range_table_col__12__0 ..................... [WARN in 1.83s]
        19:48:27  2 of 2 PASS unique_table__uuid ................................................ [PASS in 1.85s]
        19:48:27
        19:48:27  Finished running 2 tests, 1 hook in 0 hours 0 minutes and 12.86 seconds (12.86s).
        19:48:27
        19:48:27  Completed with 1 warning:
        19:48:27
        19:48:27  Warning in test dbt_utils_accepted_range_table_col__12__0 (models/ads/ads.yaml)
        19:48:27  Got 252 results, configured to warn if >0
        19:48:27
        19:48:27    compiled Code at target/compiled/model/models/table/table.yaml/dbt_utils_accepted_range_table_col__12__0.sql
        19:48:27
        19:48:27  Done. PASS=1 WARN=1 ERROR=0 SKIP=0 TOTAL=2
        19:48:27  Command `dbt test` succeeded at 07:50:02.340364 after 43.98 seconds
        19:48:27  Flushing usage events
        """,
            True,
        ),
        (
            """
        19:48:25  Concurrency: 4 threads (target='target')
        19:48:25
        19:48:25  1 of 2 START test dbt_utils_accepted_range_table_col__12__0 ................... [RUN]
        19:48:25  2 of 2 START test unique_table__uuid .......................................... [RUN]
        19:48:27  1 of 2 PASS 252 dbt_utils_accepted_range_table_col__12__0 ..................... [PASS in 1.83s]
        19:48:27  2 of 2 PASS unique_table__uuid ................................................ [PASS in 1.85s]
        19:48:27
        19:48:27  Finished running 2 tests, 1 hook in 0 hours 0 minutes and 12.86 seconds (12.86s).
        19:48:27
        19:48:27  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
        """,
            False,
        ),
        (
            """
        gibberish
        """,
            False,
        ),
    ),
)
def test_dbt_kubernetes_operator_handle_warnings(
    caplog, fake_kubernetes_execute_pod_completion, kubernetes_pod_operator_callback, log_string, should_call
):
    mock_warning_callback = Mock()
    test_operator = DbtTestKubernetesOperator(on_warning_callback=mock_warning_callback, **base_kwargs)

    if version.parse(airflow_version) >= version.Version("3.1"):
        task_instance = TaskInstance(test_operator, dag_version_id=None)
    else:
        task_instance = TaskInstance(test_operator)

    task_instance.task.pod_manager = FakePodManager(log_string)
    task_instance.task.pod = task_instance.task.remote_pod = "pod"

    context = Context(task_instance=task_instance)
    test_operator.build_and_run_cmd(context)

    if should_call:
        mock_warning_callback.assert_called_once()
    else:
        mock_warning_callback.assert_not_called()

    if "gibberish" in log_string:
        assert "Failed to scrape warning count" in caplog.text


def test_dbt_kubernetes_operator_handle_warnings_noop(
    caplog, fake_kubernetes_execute_pod_completion, kubernetes_pod_operator_callback
):
    mock_warning_callback = Mock()
    run_operator = DbtRunKubernetesOperator(on_warning_callback=mock_warning_callback, **base_kwargs)
    if version.parse(airflow_version) >= version.Version("3.1"):
        task_instance = TaskInstance(run_operator, dag_version_id=None)
    else:
        task_instance = TaskInstance(run_operator)
    context = Context(task_instance=task_instance)

    warning_handler_no_context = DbtTestWarningHandler(mock_warning_callback, run_operator, None)
    warning_handler_no_context.on_pod_completion(pod="pod")

    assert "No context provided" in caplog.text

    warning_handler = DbtTestWarningHandler(mock_warning_callback, run_operator, context)
    warning_handler.on_pod_completion(pod="pod")

    assert "Cannot handle dbt warnings for task of type" in caplog.text


def test_created_pod():
    ls_kwargs = {"env_vars": {"FOO": "BAR"}, "namespace": "foo", "append_env": False}
    ls_kwargs.update(base_kwargs)
    ls_operator = DbtLSKubernetesOperator(**ls_kwargs)
    ls_operator.hook = MagicMock()
    ls_operator.hook.is_in_cluster = False
    ls_operator.build_kube_args(context={}, cmd_flags=MagicMock())
    pod_obj = ls_operator.build_pod_request_obj()

    metadata = pod_obj.metadata
    assert metadata.labels["airflow_kpo_in_cluster"] == "False"
    assert metadata.namespace == "foo"

    container = pod_obj.spec.containers[0]
    assert container.env[0].name == "FOO"
    assert container.env[0].value == "BAR"
    assert container.env[0].value_from is None
    assert container.image == "my_image"

    expected_container_args = [
        "dbt",
        "ls",
        "--vars",
        "end_time: '{{ "
        "data_interval_end.strftime(''%Y%m%d%H%M%S'') "
        "}}'\n"
        "start_time: '{{ "
        "data_interval_start.strftime(''%Y%m%d%H%M%S'') "
        "}}'\n",
        "--no-version-check",
        "--project-dir",
        "my/dir",
    ]
    assert container.args == expected_container_args
    assert container.command == []


@pytest.mark.parametrize(
    "operator_class,kwargs,expected_cmd",
    [
        (
            DbtSeedKubernetesOperator,
            {"full_refresh": True},
            ["dbt", "seed", "--full-refresh", "--project-dir", "my/dir"],
        ),
        (
            DbtBuildKubernetesOperator,
            {"full_refresh": True},
            ["dbt", "build", "--full-refresh", "--project-dir", "my/dir"],
        ),
        (
            DbtRunKubernetesOperator,
            {"full_refresh": True},
            ["dbt", "run", "--full-refresh", "--project-dir", "my/dir"],
        ),
        (
            DbtTestKubernetesOperator,
            {},
            ["dbt", "test", "--project-dir", "my/dir"],
        ),
        (
            DbtTestKubernetesOperator,
            {"select": []},
            ["dbt", "test", "--project-dir", "my/dir"],
        ),
        (
            DbtTestKubernetesOperator,
            {"full_refresh": True, "select": ["tag:daily"], "exclude": ["tag:disabled"]},
            ["dbt", "test", "--select", "tag:daily", "--exclude", "tag:disabled", "--project-dir", "my/dir"],
        ),
        (
            DbtTestKubernetesOperator,
            {"full_refresh": True, "selector": "nightly_snowplow"},
            ["dbt", "test", "--selector", "nightly_snowplow", "--project-dir", "my/dir"],
        ),
    ],
)
def test_operator_execute_with_flags(operator_class, kwargs, expected_cmd):
    task = operator_class(
        task_id="my-task",
        project_dir="my/dir",
        **kwargs,
    )

    with (
        patch(
            "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.hook",
            is_in_cluster=False,
        ),
        patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup"),
        patch(
            "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.get_or_create_pod",
            side_effect=ValueError("Mock"),
        ) as get_or_create_pod,
    ):
        try:
            task.execute(context={})
        except ValueError as e:
            if e != get_or_create_pod.side_effect:
                raise

    pod_args = get_or_create_pod.call_args.kwargs["pod_request_obj"].to_dict()["spec"]["containers"][0]["args"]

    assert expected_cmd == pod_args


DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"


@pytest.mark.integration
def test_kubernetes_task_group():
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )

    group_id = "test_group"
    image = "test_image"
    with DAG(
        "test-id-dbt-compile",
        start_date=datetime(2022, 1, 1),
        default_args={
            "image": image,
        },
    ) as dag:
        task_group = DbtTaskGroup(
            group_id=group_id,
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "jaffle_shop",
            ),
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.KUBERNETES,
            ),
            profile_config=profile_config,
        )

    assert all(t.task_id.startswith(f"{group_id}.") for t in task_group)
    assert len(dag.tasks) == len([t for t in task_group])
    assert any(t for t in task_group if isinstance(t, DbtKubernetesBaseOperator))
    assert all(t.image == image for t in (t for t in task_group if isinstance(t, DbtKubernetesBaseOperator)))


@pytest.mark.integration
def test_kubernetes_default_args():
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )

    image = "test_image"
    with DAG(
        "test-id-dbt-compile",
        start_date=datetime(2022, 1, 1),
        default_args={"project_dir": DBT_ROOT_PATH / "jaffle_shop", "image": image, "profile_config": profile_config},
    ):
        dbt_run_operation = DbtRunOperationKubernetesOperator(
            task_id="run_macro_command",
            macro_name="macro",
        )

    assert dbt_run_operation.image == image
    assert dbt_run_operation.project_dir == DBT_ROOT_PATH / "jaffle_shop"
    assert dbt_run_operation.profile_config.target_name == profile_config.target_name


def test_kubernetes_pod_container_resources():
    """Test that the container_resources are converted to V1ResourceRequirements in the operator."""
    resources = {
        "requests": {"cpu": "100m", "memory": "128Mi"},
        "limits": {"cpu": "500m", "memory": "512Mi"},
    }
    a_operator = DbtRunOperationKubernetesOperator(
        task_id="run_macro_command",
        macro_name="macro",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        container_resources=resources,
    )
    assert isinstance(a_operator.container_resources, k8s.V1ResourceRequirements)
    assert a_operator.container_resources.to_dict()["requests"] == resources["requests"]
    assert a_operator.container_resources.to_dict()["limits"] == resources["limits"]

    b_operator = DbtRunOperationKubernetesOperator(
        task_id="run_macro_command",
        macro_name="macro",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        container_resources=k8s.V1ResourceRequirements(**resources),
    )
    assert isinstance(b_operator.container_resources, k8s.V1ResourceRequirements)
    assert b_operator.container_resources.to_dict()["requests"] == resources["requests"]
    assert b_operator.container_resources.to_dict()["limits"] == resources["limits"]

    c_operator = DbtRunOperationKubernetesOperator(
        task_id="run_macro_command",
        macro_name="macro",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
    )
    assert c_operator.container_resources is None
