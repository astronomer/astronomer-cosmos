from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.aws_ecs import (
    DbtAwsEcsBaseOperator,
    DbtBuildAwsEcsOperator,
    DbtLSAwsEcsOperator,
    DbtRunAwsEcsOperator,
    DbtRunOperationAwsEcsOperator,
    DbtSeedAwsEcsOperator,
    DbtSnapshotAwsEcsOperator,
    DbtSourceAwsEcsOperator,
    DbtTestAwsEcsOperator,
)


class ConcreteDbtAwsEcsOperator(DbtAwsEcsBaseOperator):
    base_cmd = ["cmd"]


def test_dbt_aws_ecs_operator_add_global_flags() -> None:
    """
    Check if global flags are added correctly.
    """
    dbt_base_operator = ConcreteDbtAwsEcsOperator(
        task_id="my-task",
        aws_conn_id="my-aws-conn-id",
        cluster="my-ecs-cluster",
        task_definition="my-dbt-task-definition",
        container_name="my-dbt-container-name",
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


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_ecs_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAwsEcsOperator(
        task_id="my-task",
        aws_conn_id="my-aws-conn-id",
        cluster="my-ecs-cluster",
        task_definition="my-dbt-task-definition",
        container_name="my-dbt-container-name",
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


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_ecs_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAwsEcsOperator(
        task_id="my-task",
        aws_conn_id="my-aws-conn-id",
        cluster="my-ecs-cluster",
        task_definition="my-dbt-task-definition",
        container_name="my-dbt-container-name",
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


base_kwargs = {
    "task_id": "my-task",
    "aws_conn_id": "my-aws-conn-id",
    "cluster": "my-ecs-cluster",
    "task_definition": "my-dbt-task-definition",
    "container_name": "my-dbt-container-name",
    "environment_variables": {"FOO": "BAR", "OTHER_FOO": "OTHER_BAR"},
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}

result_map = {
    "ls": DbtLSAwsEcsOperator(**base_kwargs),
    "run": DbtRunAwsEcsOperator(**base_kwargs),
    "test": DbtTestAwsEcsOperator(**base_kwargs),
    "source": DbtSourceAwsEcsOperator(**base_kwargs),
    "seed": DbtSeedAwsEcsOperator(**base_kwargs),
    "build": DbtBuildAwsEcsOperator(**base_kwargs),
    "snapshot": DbtSnapshotAwsEcsOperator(**base_kwargs),
    "run-operation": DbtRunOperationAwsEcsOperator(macro_name="some-macro", **base_kwargs),
}


def test_dbt_aws_ecs_build_command():
    """
    Check whether the dbt command is built correctly.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        if command_name not in {"run-operation", "source"}:
            assert command_operator.command == [
                "dbt",
                command_name,
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
                "--project-dir",
                "my/dir",
            ]
        elif command_name == "source":
            assert command_operator.command == [
                "dbt",
                command_name,
                "freshness",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
                "--project-dir",
                "my/dir",
            ]
        else:
            assert command_operator.command == [
                "dbt",
                command_name,
                "some-macro",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
                "--project-dir",
                "my/dir",
            ]


def test_dbt_aes_ecs_overrides_parameter():
    """
    Check whether overrides parameter passed on to EcsRunTaskOperator is built correctly.
    """

    run_operator = DbtRunAwsEcsOperator(**base_kwargs)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())

    actual_overrides = run_operator.overrides

    assert "containerOverrides" in actual_overrides
    actual_container_overrides = actual_overrides["containerOverrides"][0]
    assert actual_container_overrides["name"] == "my-dbt-container-name"
    assert isinstance(actual_container_overrides["command"], list), "`command` should be of type list"

    assert "environment" in actual_container_overrides
    actual_env = actual_container_overrides["environment"]

    expected_env_vars = [{"name": "FOO", "value": "BAR"}, {"name": "OTHER_FOO", "value": "OTHER_BAR"}]

    for expected_env_var in expected_env_vars:
        assert expected_env_var in actual_env


@patch("cosmos.operators.aws_ecs.EcsRunTaskOperator.execute")
def test_dbt_aws_ecs_build_and_run_cmd(mock_execute):
    """
    Check that building methods run correctly.
    """

    dbt_base_operator = ConcreteDbtAwsEcsOperator(
        task_id="my-task",
        aws_conn_id="my-aws-conn-id",
        cluster="my-ecs-cluster",
        task_definition="my-dbt-task-definition",
        container_name="my-dbt-container-name",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    mock_build_command = MagicMock()
    dbt_base_operator.build_command = mock_build_command

    mock_context = MagicMock()
    dbt_base_operator.build_and_run_cmd(context=mock_context)

    mock_build_command.assert_called_with(mock_context, None)
    mock_execute.assert_called_once_with(dbt_base_operator, mock_context)
