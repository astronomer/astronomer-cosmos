from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.utils.context import Context
from pendulum import datetime

from cosmos import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.operators.docker import (
    DbtBuildDockerOperator,
    DbtCloneDockerOperator,
    DbtLSDockerOperator,
    DbtRunDockerOperator,
    DbtSeedDockerOperator,
    DbtTestDockerOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping


@pytest.fixture()
def mock_docker_execute():
    with patch("cosmos.operators.docker.DockerOperator.execute") as mock_execute:
        yield mock_execute


@pytest.fixture()
def base_operator(mock_docker_execute):
    from cosmos.operators.docker import DbtDockerBaseOperator

    class ConcreteDbtDockerBaseOperator(DbtDockerBaseOperator):
        base_cmd = ["cmd"]

    return ConcreteDbtDockerBaseOperator


def test_dbt_docker_operator_add_global_flags(base_operator) -> None:
    dbt_base_operator = base_operator(
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
    assert dbt_base_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@patch("cosmos.operators.docker.DbtDockerBaseOperator.build_command")
def test_dbt_docker_operator_execute(mock_build_command, base_operator, mock_docker_execute):
    """Tests that the execute method call results in both the build_command method and the docker execute method being called."""
    operator = base_operator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    # Assert that the build_command method was called in the execution
    mock_build_command.assert_called_once()
    # Assert that the docker execute method was called in the execution
    mock_docker_execute.assert_called_once()
    assert mock_docker_execute.call_args.args[-1] == {}


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_docker_operator_get_env(p_context_to_airflow_vars: MagicMock, base_operator) -> None:
    """
    If an end user passes in a
    """
    dbt_base_operator = base_operator(
        conn_id="my_airflow_connection", task_id="my-task", image="my_image", project_dir="my/dir", append_env=False
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
    "ls": DbtLSDockerOperator(**base_kwargs),
    "run": DbtRunDockerOperator(**base_kwargs),
    "test": DbtTestDockerOperator(**base_kwargs),
    "build": DbtBuildDockerOperator(**base_kwargs),
    "seed": DbtSeedDockerOperator(**base_kwargs),
    "clone": DbtCloneDockerOperator(**base_kwargs),
}


def test_dbt_docker_build_command():
    """
    Since we know that the DockerOperator is tested, we can just test that the
    command is built correctly.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.command == [
            "dbt",
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
        ]


def test_profile_config_without_profiles_yml_raises_error(base_operator):
    with pytest.raises(CosmosValueError) as err:
        base_operator(
            conn_id="my_airflow_connection",
            task_id="my-task",
            image="my_image",
            project_dir="my/dir",
            append_env=False,
            profile_config=ProfileConfig(
                profile_name="profile_name",
                target_name="target_name",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="example_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        )

    error_message = str(err.value)
    expected_err_msg = "For ExecutionMode.DOCKER, specifying ProfileConfig only works with profiles_yml_filepath method"

    assert expected_err_msg in error_message
