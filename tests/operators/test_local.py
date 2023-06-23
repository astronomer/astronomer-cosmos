from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.local import DbtLocalBaseOperator


def test_dbt_base_operator_add_global_flags() -> None:
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
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


@pytest.mark.parametrize(
    ["skip_exception", "exception_code_returned", "expected_exception"],
    [
        (99, 99, AirflowSkipException),
        (80, 99, AirflowException),
        (None, 0, None),
    ],
    ids=[
        "Exception matches skip exception, airflow skip raised",
        "Exception does not match skip exception, airflow exception raised",
        "No exception raised",
    ],
)
def test_dbt_base_operator_exception_handling(skip_exception, exception_code_returned, expected_exception) -> None:
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
    )
    if expected_exception:
        with pytest.raises(expected_exception):
            dbt_base_operator.exception_handling(SubprocessResult(exception_code_returned, None))
    else:
        dbt_base_operator.exception_handling(SubprocessResult(exception_code_returned, None))


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_base_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
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


@patch("os.path.exists")
def test_get_profile_name(mock_os_path_exists) -> None:
    mock_os_path_exists.return_value = True

    # check that a user-specified profile name is returned when specified
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
        profile_name="default",
    )
    assert dbt_base_operator.get_profile_name("path/to/dir") == "default"

    # check that the dbt_project profile name is returned when no user-specified profile name is specified
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
    )
    with patch("pathlib.Path.read_text") as mock_read_text:
        mock_read_text.return_value = "profile: default"
        assert dbt_base_operator.get_profile_name("path/to/dir") == "default"

    # check that the default profile name is returned when no user-specified profile name is specified and no
    # dbt_project profile name is specified
    with patch("pathlib.Path.read_text") as mock_read_text:
        mock_read_text.return_value = ""
        assert dbt_base_operator.get_profile_name("path/to/dir") == "cosmos_profile"

        mock_read_text.return_value = "other_config: other_value"
        assert dbt_base_operator.get_profile_name("path/to/dir") == "cosmos_profile"

    # test that we raise an AirflowException if the profile argument is not a string
    with patch("pathlib.Path.read_text") as mock_read_text:
        mock_read_text.return_value = "profile:\n  my_key: my_value"
        with pytest.raises(AirflowException):
            dbt_base_operator.get_profile_name("path/to/dir")

    # test that we raise an AirflowException if there's no dbt_project.yml file
    mock_os_path_exists.return_value = False
    with pytest.raises(AirflowException):
        dbt_base_operator.get_profile_name("path/to/dir")


def test_get_target_name() -> None:
    # when a user specifies a target name, it should be returned
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
        target_name="default",
    )
    assert dbt_base_operator.get_target_name() == "default"

    # when a user does not specify a target name, the default target name should be returned
    dbt_base_operator = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
    )

    assert dbt_base_operator.get_target_name() == "cosmos_target"
