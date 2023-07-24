from unittest.mock import patch

from airflow.utils.context import Context

from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.operators.docker import DbtDockerBaseOperator


def test_docker_command_env() -> None:
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
                execution_mode="docker",
                dbt_executable_path="dbt",
                append_env=True,
            ),
        )

    dbt_base_operator = DbtDockerBaseOperator(
        cosmos_config=cosmos_config,
        task_id="my-task",
        image="my_image",
        models=["my_model"],
        env={
            "my_key": "my_value",
        },
    )

    with patch.dict("os.environ", {"MY_ENV_VAR": "value"}, clear=True):
        dbt_base_operator.prepare(context=Context())

        assert dbt_base_operator.command == [
            "dbt",
            "help",
            "--select",
            "my_model",
        ]

        assert dbt_base_operator.environment == {
            "my_key": "my_value",
            "MY_ENV_VAR": "value",
        }
