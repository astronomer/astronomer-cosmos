from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import airflow
import pytest
from airflow.models import DAG
from airflow.models.connection import Connection
from packaging.version import Version

from cosmos.config import ProfileConfig
from cosmos.constants import _AIRFLOW3_MAJOR_VERSION, InvocationMode
from cosmos.exceptions import CosmosValueError
from cosmos.operators.virtualenv import DbtCloneVirtualenvOperator, DbtVirtualenvBaseOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

AIRFLOW_VERSION = Version(airflow.__version__)

DBT_PROJ_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"

DAGS_FOLDER = Path(__file__).parent.parent.parent / "dev/dags/"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="fake_conn",
        profile_args={"schema": "public"},
    ),
)

real_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


def patch_execute_in_subprocess(func):
    if AIRFLOW_VERSION.major >= _AIRFLOW3_MAJOR_VERSION:
        return patch("airflow.providers.standard.utils.python_virtualenv.execute_in_subprocess")(func)
    else:
        return patch("airflow.utils.python_virtualenv.execute_in_subprocess")(func)


class ConcreteDbtVirtualenvBaseOperator(DbtVirtualenvBaseOperator):

    @property
    def base_cmd(self) -> list[str]:
        return ["cmd"]


@patch("cosmos.operators.local.AbstractDbtLocalBase._upload_sql_files")
@patch_execute_in_subprocess
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.calculate_openlineage_events_completes")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator._override_rtif")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.store_compiled_sql")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.handle_exception_subprocess")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.subprocess_hook")
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_command_without_virtualenv_dir(
    mock_get_connection,
    mock_subprocess_hook,
    mock_exception_handling,
    mock_store_compiled_sql,
    mock_override_rtif,
    mock_calculate_openlineage_events_completes,
    mock_execute,
    _upload_sql_files,
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
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        profile_config=profile_config,
        task_id="fake_task",
        install_deps=True,
        project_dir="./dev/dags/dbt/jaffle_shop",
        py_system_site_packages=False,
        pip_install_options=["--test-flag"],
        py_requirements=["dbt-postgres==1.6.0b1"],
        emit_datasets=False,
        invocation_mode=InvocationMode.SUBPROCESS,
    )
    assert venv_operator.virtualenv_dir == None
    venv_operator.run_command(cmd=["fake-dbt", "do-something"], env={}, context={"task_instance": MagicMock()})
    run_command_args = mock_subprocess_hook.run_command.call_args_list
    assert len(run_command_args) == 2
    dbt_deps = run_command_args[0].kwargs
    dbt_cmd = run_command_args[1].kwargs
    assert dbt_deps["command"][0] == dbt_cmd["command"][0]
    assert dbt_deps["command"][1] == "deps"
    assert dbt_cmd["command"][1] == "do-something"
    assert mock_execute.call_count == 2
    virtualenv_call, pip_install_call = mock_execute.call_args_list
    if AIRFLOW_VERSION.major >= _AIRFLOW3_MAJOR_VERSION:
        assert "python3" in virtualenv_call[0][0]
        assert virtualenv_call[0][0][0] == "uv"
        assert virtualenv_call[0][0][1] == "venv"
        assert pip_install_call[0][0][0] == "uv"
        assert pip_install_call[0][0][1] == "pip"
        assert pip_install_call[0][0][2] == "install"
    else:
        assert "python" in virtualenv_call[0][0][0]
        assert virtualenv_call[0][0][1] == "-m"
        assert virtualenv_call[0][0][2] == "virtualenv"
        assert "pip" in pip_install_call[0][0][0]
        assert pip_install_call[0][0][1] == "install"
    cosmos_venv_dirs = [
        f for f in os.listdir("/tmp") if os.path.isdir(os.path.join("/tmp", f)) and f.startswith("cosmos-venv")
    ]
    assert len(cosmos_venv_dirs) == 0


@patch("cosmos.operators.local.AbstractDbtLocalBase._upload_sql_files")
@patch("cosmos.operators.virtualenv.DbtVirtualenvBaseOperator._is_lock_available")
@patch("time.sleep")
@patch("cosmos.operators.virtualenv.DbtVirtualenvBaseOperator._release_venv_lock")
@patch_execute_in_subprocess
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.calculate_openlineage_events_completes")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator._override_rtif")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.store_compiled_sql")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.handle_exception_subprocess")
@patch("cosmos.operators.virtualenv.DbtLocalBaseOperator.subprocess_hook")
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_command_with_virtualenv_dir(
    mock_get_connection,
    mock_subprocess_hook,
    mock_exception_handling,
    mock_store_compiled_sql,
    mock_override_rtif,
    mock_calculate_openlineage_events_completes,
    mock_execute,
    mock_release_venv_lock,
    mock_sleep,
    mock_is_lock_available,
    _upload_sql_files,
    caplog,
):
    mock_is_lock_available.side_effect = [False, False, True]
    mock_get_connection.return_value = Connection(
        conn_id="fake_conn",
        conn_type="postgres",
        host="fake_host",
        port=5432,
        login="fake_login",
        password="fake_password",
        schema="fake_schema",
    )
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        profile_config=profile_config,
        task_id="fake_task",
        install_deps=True,
        project_dir="./dev/dags/dbt/jaffle_shop",
        py_system_site_packages=False,
        pip_install_options=["--test-flag"],
        py_requirements=["dbt-postgres==1.6.0b1"],
        emit_datasets=False,
        invocation_mode=InvocationMode.SUBPROCESS,
        virtualenv_dir=Path("mock-venv"),
    )
    venv_operator.run_command(cmd=["fake-dbt", "do-something"], env={}, context={"task_instance": MagicMock()})
    assert str(venv_operator.virtualenv_dir) == "mock-venv"
    run_command_args = mock_subprocess_hook.run_command.call_args_list
    assert len(run_command_args) == 2
    dbt_deps = run_command_args[0].kwargs
    dbt_cmd = run_command_args[1].kwargs
    assert dbt_deps["command"][0] == "mock-venv/bin/dbt"
    assert dbt_cmd["command"][0] == "mock-venv/bin/dbt"
    assert caplog.text.count("Waiting for virtualenv lock to be released") == 2
    assert mock_sleep.call_count == 2
    assert mock_is_lock_available.call_count == 3
    assert mock_release_venv_lock.call_count == 1
    cosmos_venv_dirs = [f for f in os.listdir() if f == "mock-venv"]
    assert len(cosmos_venv_dirs) == 1


def test_virtualenv_operator_append_env_is_true_by_default():
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        profile_config=profile_config,
        task_id="fake_task",
        install_deps=True,
        project_dir="./dev/dags/dbt/jaffle_shop",
        py_system_site_packages=False,
        pip_install_options=["--test-flag"],
        py_requirements=["dbt-postgres==1.6.0b1"],
        emit_datasets=False,
        invocation_mode=InvocationMode.SUBPROCESS,
    )

    assert venv_operator.append_env is True


def test_depends_on_virtualenv_dir_raises_exeption():
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="buggy_task",
    )
    venv_operator.virtualenv_dir = None
    with pytest.raises(CosmosValueError) as excepion_info:
        venv_operator._is_lock_available()
    assert str(excepion_info.value) == "Method relies on value of parameter `virtualenv_dir` which is None."


def test_clean_dir_if_temporary(tmpdir):
    tmp_filepath = Path(tmpdir / "tmpfile.txt")
    tmp_filepath.touch()
    assert tmp_filepath.exists()

    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=True,
        virtualenv_dir=tmpdir,
    )
    venv_operator.clean_dir_if_temporary()
    assert not tmp_filepath.exists()
    assert not tmpdir.exists()


@patch("cosmos.operators.virtualenv.DbtVirtualenvBaseOperator.clean_dir_if_temporary")
def test_on_kill(mock_clean_dir_if_temporary):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
    )
    venv_operator.on_kill()
    assert mock_clean_dir_if_temporary.called


@patch("cosmos.operators.virtualenv.DbtVirtualenvBaseOperator.subprocess_hook")
def test_run_subprocess(mock_subprocess_hook, tmpdir, caplog):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=tmpdir,
    )
    venv_operator.run_subprocess(["dbt", "run"], {}, "./dev/dags/dbt/jaffle_shop")
    assert len(mock_subprocess_hook.run_command.call_args_list) == 1


@patch("cosmos.operators.local.DbtLocalBaseOperator.execute", side_effect=ValueError)
@patch("cosmos.operators.virtualenv.DbtVirtualenvBaseOperator.clean_dir_if_temporary")
def test__execute_cleans_dir(mock_clean_dir_if_temporary, mock_execute, caplog):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
    )
    with pytest.raises(ValueError):
        venv_operator.execute(None)
    assert mock_clean_dir_if_temporary.called


def test__is_lock_available_returns_false(tmpdir):
    parent_pid = os.getppid()
    lockfile = tmpdir / "cosmos_virtualenv.lock"
    lockfile.write_text(str(parent_pid), encoding="utf-8")
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=tmpdir,
    )
    assert not venv_operator._is_lock_available()


def test__is_lock_available_returns_true_pid_no_longer_running(tmpdir):
    non_existent_pid = "74717471"
    lockfile = tmpdir / "cosmos_virtualenv.lock"
    lockfile.write_text(str(non_existent_pid), encoding="utf-8")
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=tmpdir,
    )
    assert venv_operator._is_lock_available()


def test__is_lock_available_returns_true_pid_no_lockfile(tmpdir):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=tmpdir,
    )
    assert venv_operator._is_lock_available()


def test__acquire_venv_lock_existing_dir(tmpdir, caplog):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=Path(tmpdir),
    )
    assert venv_operator._acquire_venv_lock() is None
    assert "Acquiring lock at" in caplog.text


def test__acquire_venv_lock_new_subdir(tmpdir, caplog):
    subdir = Path(tmpdir / "subdir")
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=subdir,
    )
    assert venv_operator._acquire_venv_lock() is None
    assert "Acquiring lock at" in caplog.text


def test__release_venv_lock_inexistent(tmpdir, caplog):
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=tmpdir,
    )
    assert venv_operator._release_venv_lock() is None
    assert "not found, perhaps deleted by other concurrent operator?" in caplog.text


def test__release_venv_lock_another_process(tmpdir, caplog):
    caplog.set_level(logging.WARNING)
    non_existent_pid = "747174"
    lockfile = tmpdir / "cosmos_virtualenv.lock"
    lockfile.write_text(str(non_existent_pid), encoding="utf-8")
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=Path(tmpdir),
    )
    assert venv_operator._release_venv_lock() is None
    assert lockfile.exists()
    assert "Lockfile owned by process of pid 747174, while operator has pid" in caplog.text


def test__release_venv_lock_current_process(tmpdir):
    parent_pid = os.getpid()
    lockfile = tmpdir / "cosmos_virtualenv.lock"
    lockfile.write_text(str(parent_pid), encoding="utf-8")
    venv_operator = ConcreteDbtVirtualenvBaseOperator(
        profile_config=profile_config,
        project_dir="./dev/dags/dbt/jaffle_shop",
        task_id="okay_task",
        is_virtualenv_dir_temporary=False,
        virtualenv_dir=Path(tmpdir),
    )
    assert venv_operator._release_venv_lock() is None
    assert not lockfile.exists()


@pytest.mark.skipif(
    AIRFLOW_VERSION < Version("2.5"),
    reason="This error is only reproducible with dag.test, which was introduced in Airflow 2.5",
)
@pytest.mark.integration
def test_integration_virtualenv_operator(caplog):
    """
    Confirm we're using the correct dbt command to run with virtualenv.
    """
    from airflow.models.dagbag import DagBag

    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.get_dag("example_virtualenv_mini")

    dag.test()

    assert "Trying to run the command:\n ['/tmp/persistent-venv2/bin/dbt', 'deps'" in caplog.text
    assert "Trying to run the command:\n ['/tmp/persistent-venv2/bin/dbt', 'seed'" in caplog.text


def test_dbt_clone_virtualenv_operator_initialisation():
    operator = DbtCloneVirtualenvOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="clone",
        dbt_cmd_flags=["--state", "/usr/local/airflow/dbt/jaffle_shop/target"],
        install_deps=True,
        append_env=True,
    )

    assert "clone" in operator.base_cmd
