import os.path
from concurrent.futures import ThreadPoolExecutor
from filecmp import dircmp
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from pendulum import datetime

from cosmos.providers.dbt.core.operators import DbtBaseOperator
from cosmos.providers.dbt.core.utils.file_syncing import has_differences


def test_dbt_base_operator_add_global_flags() -> None:
    dbt_base_operator = DbtBaseOperator(
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
        "--project-dir",
        "/tmp/dbt/dir",
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
def test_dbt_base_operator_exception_handling(
    skip_exception, exception_code_returned, expected_exception
) -> None:
    dbt_base_operator = DbtBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir="my/dir",
    )
    if expected_exception:
        with pytest.raises(expected_exception):
            dbt_base_operator.exception_handling(
                SubprocessResult(exception_code_returned, None)
            )
    else:
        dbt_base_operator.exception_handling(
            SubprocessResult(exception_code_returned, None)
        )


@patch("cosmos.providers.dbt.core.operators.context_to_airflow_vars")
def test_dbt_base_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_base_operator = DbtBaseOperator(
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


def test_first_sync(tmp_path: Path) -> None:
    project_path = Path(__file__).parent.parent.joinpath("example_project")
    dbt_base_operator = DbtBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir=str(project_path),
    )
    dbt_base_operator.tmp_path = tmp_path
    synced_path = dbt_base_operator.sync_temp_project()
    assert synced_path == tmp_path.joinpath(os.path.basename(project_path))
    comparison = dircmp(project_path, synced_path)
    assert not has_differences(comparison)


def test_update_sync(tmp_path: Path) -> None:
    project_path = Path(__file__).parent.parent.joinpath("example_project")
    dbt_base_operator = DbtBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir=str(project_path),
    )
    dbt_base_operator.tmp_path = tmp_path
    synced_path = dbt_base_operator.sync_temp_project()
    new_file = project_path.joinpath("some_file.yml")
    new_file.touch(exist_ok=True)
    new_directory = project_path.joinpath("macros")
    new_directory.mkdir(exist_ok=True)
    dbt_base_operator.sync_temp_project()
    comparison = dircmp(project_path, synced_path)
    assert not has_differences(comparison)
    # Tear down test artifacts
    os.remove(new_file)
    new_directory.rmdir()


def test_multiple_tasks_sync(tmp_path: Path) -> None:
    """I'm not entirely confident that this test does what it should, integration testing mandatory."""
    project_path = Path(__file__).parent.parent.joinpath("example_project")
    dbt_base_operator_1 = DbtBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        project_dir=str(project_path),
    )
    dbt_base_operator_2 = DbtBaseOperator(
        conn_id="my_airflow_connection",
        task_id="another-task",
        project_dir=str(project_path),
    )
    dbt_base_operator_1.tmp_path = tmp_path
    dbt_base_operator_2.tmp_path = tmp_path
    synced_path = dbt_base_operator_1.sync_temp_project()
    new_file = project_path.joinpath("some_file.yml")
    new_file.touch(exist_ok=True)
    with ThreadPoolExecutor(2) as tpe:
        tpe.submit(dbt_base_operator_1.sync_temp_project)
        new_directory = project_path.joinpath("macros")
        new_directory.mkdir(exist_ok=True)
        tpe.submit(dbt_base_operator_2.sync_temp_project)
    comparison = dircmp(project_path, synced_path)
    assert not has_differences(comparison)
    # Tear down test artifacts
    os.remove(new_file)
    new_directory.rmdir()
