from unittest.mock import patch

from cosmos.operators.virtualenv import DbtVirtualenvBaseOperator
from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, ExecutionConfig

from airflow.utils.context import Context


@patch("airflow.utils.python_virtualenv.execute_in_subprocess")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.execute")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.subprocess_hook")
def test_run_command(
    mock_subprocess_hook,
    mock_super_execute,
    mock_execute_in_subprocess,
):
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

    venv_operator = DbtVirtualenvBaseOperator(
        cosmos_config=cosmos_config,
        task_id="fake_task",
        py_system_site_packages=False,
        py_requirements=["dbt-postgres==1.6.0b1"],
    )

    venv_operator.execute(Context())
    assert mock_super_execute.call_count == 1
    assert mock_execute_in_subprocess.call_count == 2

    create_venv_args = mock_execute_in_subprocess.call_args_list[0]
    install_deps_args = mock_execute_in_subprocess.call_args_list[1]

    assert "virtualenv" in create_venv_args[0][0]
    assert "dbt-postgres==1.6.0b1" in install_deps_args[0][0]
