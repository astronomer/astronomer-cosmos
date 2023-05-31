from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from pendulum import datetime

from cosmos.providers.dbt.core.operators.local import DbtLocalBaseOperator


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


@patch("cosmos.providers.dbt.core.operators.base.context_to_airflow_vars")
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
        profile_vars={
            "SNOWFLAKE_USER": "my_user_id",
            "SNOWFLAKE_PASSWORD": "supersecure123",
            "SNOWFLAKE_ACCOUNT": "my_account",
            "SNOWFLAKE_ROLE": None,
            "SNOWFLAKE_DATABASE": "my_database",
            "SNOWFLAKE_WAREHOUSE": None,
            "SNOWFLAKE_SCHEMA": "jaffle_shop",
        },
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
        "SNOWFLAKE_USER": "my_user_id",
        "SNOWFLAKE_PASSWORD": "supersecure123",
        "SNOWFLAKE_ACCOUNT": "my_account",
        "SNOWFLAKE_DATABASE": "my_database",
        "SNOWFLAKE_SCHEMA": "jaffle_shop",
    }
    assert env == expected_env


@patch("os.path.exists")
def test_user_supplied_profiles(
    os_path_exists: MagicMock,
    capfd: pytest.CaptureFixture[str],
) -> None:
    "Ensures that user supplied profiles log a warning and aren't used"
    op = DbtLocalBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
    )

    os_path_exists.return_value = True

    with patch("os.remove") as os_remove:
        op.handle_profiles_yml(project_dir="my_dir")
        # ensure that the file is removed
        assert os_remove.call_count == 1

    # ensure that a warning is logged
    captured = capfd.readouterr()
    assert "WARNING" in captured.out and "profiles.yml" in captured.out
