from unittest.mock import patch

from cosmos.operators.virtualenv import DbtVirtualenvBaseOperator

from airflow.models.connection import Connection

from cosmos.config import ProfileConfig

from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="fake_conn",
        profile_args={"schema": "public"},
    ),
)


@patch("airflow.utils.python_virtualenv.execute_in_subprocess")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.store_compiled_sql")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.exception_handling")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.subprocess_hook")
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_command(
    mock_get_connection,
    mock_subprocess_hook,
    mock_exception_handling,
    mock_store_compiled_sql,
    mock_execute,
):
    mock_get_connection.return_value = Connection(
        conn_id="fake_conn",
        conn_type="postgres",
        host="fake_host",
        port=5432,
        login="fake_login",
        password="fake_password",
        schema="fake_schema",
    )
    venv_operator = DbtVirtualenvBaseOperator(
        profile_config=profile_config,
        task_id="fake_task",
        install_deps=True,
        project_dir="./dev/dags/dbt/jaffle_shop",
        py_system_site_packages=False,
        py_requirements=["dbt-postgres==1.6.0b1"],
    )
    assert venv_operator._venv_tmp_dir is None  # Otherwise we are creating empty directories during DAG parsing time
    # and not deleting them
    venv_operator.run_command(cmd=["fake-dbt", "do-something"], env={}, context={})
    run_command_args = mock_subprocess_hook.run_command.call_args_list
    assert len(run_command_args) == 3
    python_cmd = run_command_args[0]
    dbt_deps = run_command_args[1]
    dbt_cmd = run_command_args[2]
    assert python_cmd[0][0][0].endswith("/bin/python")
    assert python_cmd[0][-1][-1] == "from importlib.metadata import version; print(version('dbt-core'))"
    assert dbt_deps[0][0][-1] == "deps"
    assert dbt_deps[0][0][0].endswith("/bin/dbt")
    assert dbt_deps[0][0][0] == dbt_cmd[0][0][0]
    assert dbt_cmd[0][0][1] == "do-something"
    assert mock_execute.call_count == 2
