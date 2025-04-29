from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow import __version__ as airflow_version
from airflow.models import TaskInstance
from airflow.utils.context import Context, context_merge
from packaging import version
from pendulum import datetime

from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtCloneKubernetesOperator,
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtSourceKubernetesOperator,
    DbtTestKubernetesOperator,
    DbtTestWarningHandler,
)


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

if version.parse(airflow_version) == version.parse("2.4"):
    base_kwargs["name"] = "some-pod-name"

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


@pytest.mark.parametrize(
    ("log_string", "should_call"),
    (
        (
            """
        19:48:25  Concurrency: 4 threads (target='target')
        19:48:25
        19:48:25  1 of 2 START test dbt_utils_accepted_range_table_col__12__0 ................... [RUN]
        19:48:25  2 of 2 START test unique_table__uuid .......................................... [RUN]
        19:48:27  1 of 2 WARN 252 dbt_utils_accepted_range_table_col__12__0 ..................... [WARN 117 in 1.83s]
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

    with patch(
        "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.hook",
        is_in_cluster=False,
    ), patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup"), patch(
        "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.get_or_create_pod",
        side_effect=ValueError("Mock"),
    ) as get_or_create_pod:
        try:
            task.execute(context={})
        except ValueError as e:
            if e != get_or_create_pod.side_effect:
                raise

    pod_args = get_or_create_pod.call_args.kwargs["pod_request_obj"].to_dict()["spec"]["containers"][0]["args"]

    assert expected_cmd == pod_args
