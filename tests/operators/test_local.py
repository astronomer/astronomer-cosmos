from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.local import (
    DbtLocalBaseOperator,
    DbtTestLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSeedLocalOperator,
    DbtRunLocalOperator,
    DbtLSLocalOperator,
)
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, ExecutionConfig


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
    skip_exception,
    exception_code_returned,
    expected_exception,
) -> None:
    with patch("pathlib.Path.exists", return_value=True):
        cosmos_config = CosmosConfig(
            project_config=ProjectConfig(
                dbt_project="my/dir",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml="my/profiles.yml",
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path="my/dbt",
                dbt_cli_flags=["--full-refresh"],
                skip_exit_code=skip_exception,
            ),
        )

    dbt_base_operator = DbtLocalBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
    )
    if expected_exception:
        with pytest.raises(expected_exception):
            dbt_base_operator.exception_handling(
                Context(execution_date=datetime(2023, 2, 15, 12, 30)),
                FullOutputSubprocessResult(exception_code_returned, [], []),
            )
    else:
        dbt_base_operator.exception_handling(
            Context(execution_date=datetime(2023, 2, 15, 12, 30)),
            FullOutputSubprocessResult(exception_code_returned, [], []),
        )


def test_store_compiled_sql() -> None:
    with patch("pathlib.Path.exists", return_value=True):
        cosmos_config = CosmosConfig(
            project_config=ProjectConfig(
                dbt_project="my/dir",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml="my/profiles.yml",
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path="my/dbt",
                dbt_cli_flags=["--full-refresh"],
            ),
        )

    dbt_base_operator = DbtLocalBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
        should_store_compiled_sql=False,
    )

    # here we just need to call the method to make sure it doesn't raise an exception
    dbt_base_operator.store_compiled_sql(
        tmp_project_dir="my/dir",
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )

    dbt_base_operator = DbtLocalBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
        should_store_compiled_sql=True,
    )

    # here we call the method and see if it tries to access the context["ti"]
    # it should, and it should raise a KeyError because we didn't pass in a ti
    with pytest.raises(KeyError):
        dbt_base_operator.store_compiled_sql(
            tmp_project_dir="my/dir",
            context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
        )


@pytest.mark.parametrize(
    ["dbt_operator", "expected_command"],
    [
        (DbtTestLocalOperator, "test"),
        (DbtSnapshotLocalOperator, "snapshot"),
        (DbtSeedLocalOperator, "seed"),
        (DbtRunLocalOperator, "run"),
        (DbtLSLocalOperator, "ls"),
    ],
    ids=[
        "DbtTestLocalOperator",
        "DbtSnapshotLocalOperator",
        "DbtSeedLocalOperator",
        "DbtRunLocalOperator",
        "DbtLSLocalOperator",
    ],
)
def test_dbt_local_operator_command(dbt_operator, expected_command) -> None:
    with patch("pathlib.Path.exists", return_value=True):
        cosmos_config = CosmosConfig(
            project_config=ProjectConfig(
                dbt_project="my/dir",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml="my/profiles.yml",
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path="my/dbt",
                dbt_cli_flags=["--full-refresh"],
            ),
        )

    dbt_local_operator = dbt_operator(
        cosmos_config=cosmos_config,
        task_id="my-task",
    )

    assert dbt_local_operator.build_cmd(["--my-flag"]) == [
        "my/dbt",
        expected_command,
        "--my-flag",
        "--full-refresh",
    ]
