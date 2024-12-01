import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pkg_resources
import pytest
from airflow.utils.context import Context
from pendulum import datetime

try:
    from cosmos.operators.gcp_cloud_run_job import (
        DbtBuildGcpCloudRunJobOperator,
        DbtCloneGcpCloudRunJobOperator,
        DbtGcpCloudRunJobBaseOperator,
        DbtLSGcpCloudRunJobOperator,
        DbtRunGcpCloudRunJobOperator,
        DbtRunOperationGcpCloudRunJobOperator,
        DbtSeedGcpCloudRunJobOperator,
        DbtSnapshotGcpCloudRunJobOperator,
        DbtSourceGcpCloudRunJobOperator,
        DbtTestGcpCloudRunJobOperator,
    )

    class ConcreteDbtGcpCloudRunJobOperator(DbtGcpCloudRunJobBaseOperator):
        base_cmd = ["cmd"]

except (ImportError, AttributeError):
    DbtGcpCloudRunJobBaseOperator = None


BASE_KWARGS = {
    "task_id": "my-task",
    "project_id": "my-gcp-project-id",
    "region": "europe-west1",
    "job_name": "my-fantastic-dbt-job",
    "environment_variables": {"FOO": "BAR", "OTHER_FOO": "OTHER_BAR"},
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def skip_on_empty_operator(test_func):
    """
    Skip the test if DbtGcpCloudRunJob operators couldn't be imported.
    It is required as some tests don't rely on those operators and in this case we need to avoid throwing an exception.
    """
    return pytest.mark.skipif(
        DbtGcpCloudRunJobBaseOperator is None, reason="DbtGcpCloudRunJobBaseOperator could not be imported"
    )(test_func)


def test_overrides_missing():
    """
    The overrides parameter needed to pass the dbt command was added in apache-airflow-providers-google==10.11.0.
    We need to check if the parameter is actually present in required version.
    """
    required_version = "10.11.0"
    package_name = "apache-airflow-providers-google"

    from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

    installed_version = pkg_resources.get_distribution(package_name).version
    init_signature = inspect.signature(CloudRunExecuteJobOperator.__init__)

    if pkg_resources.parse_version(installed_version) < pkg_resources.parse_version(required_version):
        assert "overrides" not in init_signature.parameters
    else:
        assert "overrides" in init_signature.parameters


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_operator_add_global_flags() -> None:
    """
    Check if global flags are added correctly.
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
    )
    assert dbt_base_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@skip_on_empty_operator
@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_gcp_cloud_run_job_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_base_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
    }
    assert env == expected_env


@skip_on_empty_operator
@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_gcp_cloud_run_job_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        "FOO": "foo",
        ("tuple", "key"): "some_value",
    }
    expected_env = {"start_date": "20220101", "end_date": "20220102", "some_path": Path(__file__), "FOO": "BAR"}
    dbt_base_operator.build_command(context=MagicMock())

    assert dbt_base_operator.environment_variables == expected_env


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_build_command():
    """
    Check whether the dbt command is built correctly.
    """

    result_map = {
        "ls": DbtLSGcpCloudRunJobOperator(**BASE_KWARGS),
        "run": DbtRunGcpCloudRunJobOperator(**BASE_KWARGS),
        "test": DbtTestGcpCloudRunJobOperator(**BASE_KWARGS),
        "seed": DbtSeedGcpCloudRunJobOperator(**BASE_KWARGS),
        "build": DbtBuildGcpCloudRunJobOperator(**BASE_KWARGS),
        "snapshot": DbtSnapshotGcpCloudRunJobOperator(**BASE_KWARGS),
        "source": DbtSourceGcpCloudRunJobOperator(**BASE_KWARGS),
        "clone": DbtCloneGcpCloudRunJobOperator(**BASE_KWARGS),
        "run-operation": DbtRunOperationGcpCloudRunJobOperator(macro_name="some-macro", **BASE_KWARGS),
    }

    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        if command_name not in ("run-operation", "source"):
            assert command_operator.command == [
                "dbt",
                command_name,
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        elif command_name == "run-operation":
            assert command_operator.command == [
                "dbt",
                command_name,
                "some-macro",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        else:
            assert command_operator.command == [
                "dbt",
                command_name,
                "freshness",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_overrides_parameter():
    """
    Check whether overrides parameter passed on to CloudRunExecuteJobOperator is built correctly.
    """

    run_operator = DbtRunGcpCloudRunJobOperator(**BASE_KWARGS)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())

    actual_overrides = run_operator.overrides

    assert "container_overrides" in actual_overrides
    actual_container_overrides = actual_overrides["container_overrides"][0]

    assert isinstance(actual_container_overrides["args"], list), "`args` should be of type list"

    assert "env" in actual_container_overrides
    actual_env = actual_container_overrides["env"]

    expected_env_vars = [{"name": "FOO", "value": "BAR"}, {"name": "OTHER_FOO", "value": "OTHER_BAR"}]

    for expected_env_var in expected_env_vars:
        assert expected_env_var in actual_env


@skip_on_empty_operator
@patch("cosmos.operators.gcp_cloud_run_job.CloudRunExecuteJobOperator.execute")
def test_dbt_gcp_cloud_run_job_build_and_run_cmd(mock_execute):
    """
    Check that building methods run correctly.
    """

    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    mock_build_command = MagicMock()
    dbt_base_operator.build_command = mock_build_command

    mock_context = MagicMock()
    dbt_base_operator.build_and_run_cmd(context=mock_context)

    mock_build_command.assert_called_with(mock_context, None)
    mock_execute.assert_called_once_with(dbt_base_operator, mock_context)
