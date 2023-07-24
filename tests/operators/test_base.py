from unittest.mock import patch, MagicMock

import pytest
from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.base import DbtBaseOperator
from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, ExecutionConfig


@pytest.fixture()
def path_patch():  # type: ignore
    "Ensures that pathlib.Path.exists returns True"
    with patch("pathlib.Path.exists", return_value=True):
        yield


@patch.dict("os.environ", {"MY_ENV_VAR": "value"}, clear=True)
@patch("cosmos.operators.base.context_to_airflow_vars")
def test_get_env(context_to_airflow_vars_mock: MagicMock, path_patch) -> None:
    "Tests that the get_env function returns the correct env dict."
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
            append_env=True,
        ),
    )

    dbt_base_operator = DbtBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
        env={
            "my_key": "my_value",
        },
    )

    context_to_airflow_vars_mock.return_value = {"START_DATE": "2023-02-15 12:30:00"}

    env = dbt_base_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )

    assert env == {
        "my_key": "my_value",
        "START_DATE": "2023-02-15 12:30:00",
        "MY_ENV_VAR": "value",
    }


def test_build_cmd(path_patch) -> None:
    "Tests that the build_cmd function returns the correct command."
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

    dbt_base_operator = DbtBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
    )

    cmd = dbt_base_operator.build_cmd(["--my-flag"])

    assert cmd == [
        "my/dbt",
        "help",
        "--my-flag",
        "--full-refresh",
    ]
