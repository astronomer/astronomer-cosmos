import base64
import json
import logging
import os
import shutil
import sys
import tempfile
import zlib
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
from airflow import DAG
from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowException, AirflowSkipException

try:  # For Airflow 3
    from airflow.providers.standard.hooks.subprocess import SubprocessResult
except ImportError:  # For Airflow 2
    from airflow.hooks.subprocess import SubprocessResult
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from packaging import version
from pendulum import datetime

import cosmos.dbt.runner as dbt_runner
from cosmos import cache
from cosmos.config import ProfileConfig
from cosmos.constants import PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS, InvocationMode
from cosmos.dbt.parser.output import (
    parse_number_of_warnings_subprocess,
)
from cosmos.exceptions import CosmosDbtRunError, CosmosValueError
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.operators.local import (
    AbstractDbtLocalBase,
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtCompileLocalOperator,
    DbtDocsAzureStorageLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from tests.utils import new_test_dag
from tests.utils import test_dag as run_test_dag

DBT_PROJ_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
MINI_DBT_PROJ_DIR = Path(__file__).parent.parent / "sample/mini"
MINI_DBT_PROJ_DIR_FAILING_SCHEMA = MINI_DBT_PROJ_DIR / "schema_failing_test.yml"
MINI_DBT_PROJ_PROFILE = MINI_DBT_PROJ_DIR / "profiles.yml"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=MagicMock(),
)

real_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)

mini_profile_config = ProfileConfig(profile_name="mini", target_name="dev", profiles_yml_filepath=MINI_DBT_PROJ_PROFILE)


@pytest.fixture
def failing_test_dbt_project(tmp_path):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_dir_path = Path(tmp_dir.name) / "mini"
    shutil.copytree(MINI_DBT_PROJ_DIR, tmp_dir_path)
    target_schema = tmp_dir_path / "models/schema.yml"
    target_schema.exists() and os.remove(target_schema)
    shutil.copy(MINI_DBT_PROJ_DIR_FAILING_SCHEMA, target_schema)
    yield tmp_dir_path
    tmp_dir.cleanup()


class ConcreteDbtLocalBaseOperator(DbtLocalBaseOperator):
    base_cmd = ["cmd"]


def test_install_deps_in_empty_dir_becomes_false(tmpdir):
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config, task_id="my-task", project_dir=tmpdir, install_deps=True
    )
    assert not dbt_base_operator.install_deps


def test_dbt_base_operator_add_global_flags() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
        select=["my_first_model", "my_second_model"],
    )
    assert dbt_base_operator.add_global_flags() == [
        "--select",
        "my_first_model my_second_model",
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


def test_dbt_local_operator_append_env_is_true_by_default() -> None:
    dbt_local_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
        select=["my_first_model", "my_second_model"],
    )

    assert dbt_local_operator.append_env == True


def test_dbt_base_operator_add_user_supplied_flags() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        dbt_cmd_flags=["--full-refresh"],
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    assert cmd[-2] == "cmd"
    assert cmd[-1] == "--full-refresh"


def test_dbt_base_operator_add_user_supplied_global_flags() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        dbt_cmd_global_flags=["--cache-selected-only"],
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    assert cmd[-2] == "--cache-selected-only"
    assert cmd[-1] == "cmd"


@pytest.mark.parametrize(
    "invocation_mode, invoke_dbt_method, handle_exception_method",
    [
        (InvocationMode.SUBPROCESS, "run_subprocess", "handle_exception_subprocess"),
        (InvocationMode.DBT_RUNNER, "run_dbt_runner", "handle_exception_dbt_runner"),
    ],
)
def test_dbt_base_operator_set_invocation_methods(invocation_mode, invoke_dbt_method, handle_exception_method):
    """Tests that the right methods are mapped to DbtLocalBaseOperator.invoke_dbt and
    DbtLocalBaseOperator.handle_exception when a known invocation mode passed.
    """
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config, task_id="my-task", project_dir="my/dir", invocation_mode=invocation_mode
    )
    assert dbt_base_operator.invoke_dbt.__name__ == invoke_dbt_method
    assert dbt_base_operator.handle_exception.__name__ == handle_exception_method


def test_dbt_base_operator_invalid_invocation_mode():
    """Tests that an invalid invocation_mode raises a ValueError when invoke_dbt or handle_exception is accessed."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
    )
    operator.invocation_mode = "invalid_mode"

    with pytest.raises(ValueError, match="Invalid invocation mode: invalid_mode"):
        _ = operator.invoke_dbt

    with pytest.raises(ValueError, match="Invalid invocation mode: invalid_mode"):
        _ = operator.handle_exception


@pytest.mark.parametrize(
    "can_import_dbt, invoke_dbt_method, handle_exception_method",
    [
        (False, "run_subprocess", "handle_exception_subprocess"),
        (True, "run_dbt_runner", "handle_exception_dbt_runner"),
    ],
)
def test_dbt_base_operator_discover_invocation_mode(can_import_dbt, invoke_dbt_method, handle_exception_method):
    """Tests that the right methods are mapped to DbtLocalBaseOperator.invoke_dbt and
    DbtLocalBaseOperator.handle_exception if dbt can be imported or not.
    """
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config, task_id="my-task", project_dir="my/dir"
    )
    with patch.dict(sys.modules, {"dbt.cli.main": MagicMock()} if can_import_dbt else {"dbt.cli.main": None}):
        dbt_base_operator = ConcreteDbtLocalBaseOperator(
            profile_config=profile_config, task_id="my-task", project_dir="my/dir"
        )
        dbt_base_operator._discover_invocation_mode()
        assert dbt_base_operator.invocation_mode == (
            InvocationMode.DBT_RUNNER if can_import_dbt else InvocationMode.SUBPROCESS
        )
        assert dbt_base_operator.invoke_dbt.__name__ == invoke_dbt_method
        assert dbt_base_operator.handle_exception.__name__ == handle_exception_method


@pytest.mark.parametrize(
    "indirect_selection_type",
    [None, "cautious", "buildable", "empty"],
)
def test_dbt_base_operator_use_indirect_selection(indirect_selection_type) -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        indirect_selection=indirect_selection_type,
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    if indirect_selection_type:
        assert cmd[-2] == "--indirect-selection"
        assert cmd[-1] == indirect_selection_type
    else:
        assert cmd[0].endswith("dbt")
        assert cmd[1] == "cmd"


def test_dbt_base_operator_run_dbt_runner_cannot_import():
    """Tests that the right error message is raised if dbtRunner cannot be imported."""
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.DBT_RUNNER,
    )
    expected_error_message = "Could not import dbt core. Ensure that dbt-core >= v1.5 is installed and available in the environment where the operator is running."
    with patch.dict(sys.modules, {"dbt.cli.main": None}):
        with pytest.raises(CosmosDbtRunError, match=expected_error_message):
            dbt_base_operator.run_dbt_runner(command=["cmd"], env={}, cwd="some-project")


@patch("cosmos.dbt.project.os.environ")
@patch("cosmos.dbt.project.os.chdir")
def test_dbt_base_operator_run_dbt_runner(mock_chdir, mock_environ):
    """Tests that dbtRunner.invoke() is called with the expected cli args, that the
    cwd is changed to the expected directory, and env variables are set."""
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.DBT_RUNNER,
    )
    full_dbt_cmd = ["dbt", "run", "some_model"]
    env_vars = {"VAR1": "value1", "VAR2": "value2"}

    mock_dbt = MagicMock()
    with patch.dict(sys.modules, {"dbt.cli.main": mock_dbt}):
        dbt_base_operator.run_dbt_runner(command=full_dbt_cmd, env=env_vars, cwd="some-dir")

    mock_dbt_runner = mock_dbt.dbtRunner.return_value
    expected_cli_args = ["run", "some_model"]
    # Assert dbtRunner.invoke was called with the expected cli args
    assert mock_dbt_runner.invoke.call_count == 1
    assert mock_dbt_runner.invoke.call_args[0][0] == expected_cli_args
    # Assert cwd was changed to the expected directory
    assert mock_chdir.call_count == 2
    assert mock_chdir.call_args_list[0][0][0] == "some-dir"
    # Assert env variables were updated
    assert mock_environ.update.call_count == 1
    assert mock_environ.update.call_args[0][0] == env_vars


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
def test_dbt_base_operator_exception_handling_subprocess(
    skip_exception, exception_code_returned, expected_exception
) -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.SUBPROCESS,
    )
    if expected_exception:
        with pytest.raises(expected_exception):
            dbt_base_operator.handle_exception(SubprocessResult(exception_code_returned, None))
    else:
        dbt_base_operator.handle_exception(SubprocessResult(exception_code_returned, None))


def test_dbt_base_operator_handle_exception_dbt_runner_unhandled_error():
    """Tests that an AirflowException is raised if the dbtRunner result is not successful with an unhandled error."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=MagicMock(),
        task_id="my-task",
        project_dir="my/dir",
    )
    result = MagicMock()
    result.success = False
    result.exception = "some exception"
    expected_error_message = "dbt invocation did not complete with unhandled error: some exception"

    with pytest.raises(CosmosDbtRunError, match=expected_error_message):
        operator.handle_exception_dbt_runner(result)


@patch("cosmos.dbt.runner.extract_message_by_status", return_value=(["node1", "node2"], ["error1", "error2"]))
def test_dbt_base_operator_handle_exception_dbt_runner_handled_error(mock_extract_dbt_runner_issues):
    """Tests that an AirflowException is raised if the dbtRunner result is not successful and with handled errors."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=MagicMock(),
        task_id="my-task",
        project_dir="my/dir",
    )
    result = MagicMock()
    result.success = False
    result.exception = None

    expected_error_message = "dbt invocation completed with errors: node1: error1\nnode2: error2"

    with pytest.raises(CosmosDbtRunError, match=expected_error_message):
        operator.handle_exception_dbt_runner(result)

    mock_extract_dbt_runner_issues.assert_called_once()


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_base_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config, task_id="my-task", project_dir="my/dir", append_env=False
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


@patch("cosmos.operators.local.extract_log_issues")
def test_dbt_test_local_operator_invocation_mode_methods(mock_extract_log_issues):
    # test subprocess invocation mode
    operator = DbtTestLocalOperator(
        profile_config=profile_config,
        invocation_mode=InvocationMode.SUBPROCESS,
        task_id="my-task",
        project_dir="my/dir",
    )
    operator._set_test_result_parsing_methods()
    assert operator.parse_number_of_warnings == parse_number_of_warnings_subprocess
    result = MagicMock(full_output="some output")
    operator.extract_issues(result)
    mock_extract_log_issues.assert_called_once_with("some output")

    # test dbt runner invocation mode
    operator = DbtTestLocalOperator(
        profile_config=profile_config,
        invocation_mode=InvocationMode.DBT_RUNNER,
        task_id="my-task",
        project_dir="my/dir",
    )
    operator._set_test_result_parsing_methods()
    assert operator.extract_issues == dbt_runner.extract_message_by_status
    assert operator.parse_number_of_warnings == dbt_runner.parse_number_of_warnings


@pytest.mark.skipif(
    version.parse(airflow_version) >= version.parse("2.10")
    or version.parse(airflow_version) in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow inlets and outlets do not work by default in Airflow 2.9.0 and 2.9.1. \n"
    "From Airflow 2.10 onwards, we started using DatasetAlias, which changed this behaviour.",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets(caplog):
    from airflow.datasets import Dataset

    project_dir = Path(__file__).parent.parent.parent / "dev/dags/dbt/altered_jaffle_shop"

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=project_dir,
            task_id="seed",
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
        )
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=project_dir,
            task_id="run",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=project_dir,
            task_id="test",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        seed_operator >> run_operator >> test_operator

    run_test_dag(dag)

    assert run_operator.inlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.raw_customers", extra=None)]
    assert run_operator.outlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.inlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.outlets == []


@pytest.mark.skipif(version.parse(airflow_version).major >= 3, reason="This test is specific for Airflow 2.10 and 2.11")
@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.10"),
    reason="From Airflow 2.10 onwards, we started using DatasetAlias, which changed this behaviour.",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets_airflow_210(caplog):
    try:
        from airflow.models.asset import AssetAliasModel
    except ModuleNotFoundError:
        from airflow.models.dataset import DatasetAliasModel as AssetAliasModel
    from sqlalchemy.orm.exc import FlushError

    with DAG("test_id_1", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dag=dag,
            emit_datasets=False,
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
        )
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        seed_operator >> run_operator >> test_operator

    assert seed_operator.outlets == []  # because emit_datasets=False,
    assert run_operator.outlets == [AssetAliasModel(name="test_id_1__run")]
    assert test_operator.outlets == [AssetAliasModel(name="test_id_1__test")]

    with pytest.raises(FlushError):
        run_test_dag(dag, custom_tester=True)
        # This is a known limitation of Airflow 2.10.0 and 2.10.1
        # https://github.com/apache/airflow/issues/42495

        # When this is solved, we can uncomment the following:
        # dag_run, session = run_test_dag(dag)

        # Once this issue is solved, we should do some type of check on the actual datasets being emitted,
        # so we guarantee Cosmos is backwards compatible via tests using something along the lines or an alternative,
        # based on the resolution of the issue logged in Airflow:
        # dataset_model = session.scalars(select(DatasetModel).where(DatasetModel.uri == "<something>"))
        # assert dataset_model == 1


@pytest.mark.integration
def test_test_operator_do_not_register_outlets(caplog):
    with DAG("test_id_1", start_date=datetime(2022, 1, 1)) as dag:
        DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )

    new_test_dag(dag)
    assert "Outlets: []" in caplog.text


@pytest.mark.skipif(
    version.parse(airflow_version) < version.Version("3.0.0"),
    reason="From Airflow 3.0 onwards, we started using AssetAlias, which changed the original behaviour",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets_airflow_3_onwards(caplog):
    with DAG("test_id_1", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dag=dag,
            emit_datasets=False,
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
        )
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        seed_operator >> run_operator >> test_operator

    caplog.clear()

    new_test_dag(dag)
    assert "Assigning outlets with AssetAlias in Airflow 3" in caplog.text
    assert (
        "Outlets: [Asset(name='postgres://0.0.0.0:5432/postgres/public/stg_customers', uri='postgres://0.0.0.0:5432/postgres/public/stg_customers'"
        in caplog.text
    )


@pytest.mark.skipif(
    version.parse(airflow_version).major < 3,
    reason="Airflow 3.0 only supports assets when setting enable_dataset_alias=True (default)",
)
@pytest.mark.integration
@patch("cosmos.settings.enable_dataset_alias", 0)
def test_run_operator_dataset_with_airflow_3_and_enabled_dataset_alias_false_fails(caplog):
    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=Path(__file__).parent.parent.parent / "dev/dags/dbt/altered_jaffle_shop",
            task_id="run",
            dbt_cmd_flags=["--models", "customers"],
            install_deps=True,
            append_env=True,
        )
        run_operator

    caplog.set_level(logging.ERROR)
    caplog.clear()
    run_test_dag(dag, expect_success=False)

    assert "ERROR" in caplog.text
    assert "To emit datasets with Airflow 3, the setting `enable_dataset_alias` must be True (default)." in caplog.text


@patch("cosmos.settings.enable_dataset_alias", 0)
@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.10"),
    reason="From Airflow 2.10 onwards, we started using DatasetAlias, which changed this behaviour.",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets_airflow_210_onwards_disabled_via_envvar(caplog):
    with DAG("test_id_2", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dag=dag,
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
    assert run_operator.outlets == []


@pytest.mark.skipif(
    version.parse(airflow_version) not in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow 2.9.0 and 2.9.1 have a breaking change in Dataset URIs",
    # https://github.com/apache/airflow/issues/39486
)
@pytest.mark.integration
def test_run_operator_dataset_emission_is_skipped(caplog):
    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
            emit_datasets=False,
        )
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
            emit_datasets=False,
        )

        seed_operator >> run_operator

    run_test_dag(dag)

    assert run_operator.inlets == []
    assert run_operator.outlets == []


@pytest.mark.skipif(
    version.parse(airflow_version) in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow inlets and outlets do not work by default in Airflow 2.9.0 and 2.9.1",
)
@pytest.mark.skipif(
    version.parse(airflow_version) >= version.parse("3"),
    reason="We do not support emitting assets with Airflow 3.0 without dataset alias.",
)
@pytest.mark.integration
@patch("cosmos.settings.enable_dataset_alias", 0)
def test_run_operator_dataset_url_encoded_names_in_airflow2(caplog):
    try:
        from airflow.sdk.definitions.asset import Dataset
    except ImportError:
        from airflow.datasets import Dataset

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=Path(__file__).parent.parent.parent / "dev/dags/dbt/altered_jaffle_shop",
            task_id="run",
            dbt_cmd_flags=["--models", "ｍｕｌｔｉｂｙｔｅ"],
            install_deps=True,
            append_env=True,
        )
        run_operator

    run_test_dag(dag)

    assert run_operator.outlets == [
        Dataset(
            uri="postgres://0.0.0.0:5432/postgres.public.%EF%BD%8D%EF%BD%95%EF%BD%8C%EF%BD%94%EF%BD%89%EF%BD%82%EF%BD%99%EF%BD%94%EF%BD%85",
            extra=None,
        )
    ]


@pytest.mark.skipif(
    version.parse(airflow_version) in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow inlets and outlets do not work by default in Airflow 2.9.0 and 2.9.1",
)
@pytest.mark.skipif(
    version.parse(airflow_version) >= version.parse("3"),
    reason="We do not support emitting assets with Airflow 3.0 without dataset alias.",
)
@pytest.mark.integration
@patch("cosmos.settings.use_dataset_airflow3_uri_standard", 1)
@patch("cosmos.settings.enable_dataset_alias", 0)
def test_run_operator_dataset_url_encoded_names_in_airflow2_with_airflow3_uri(caplog):
    try:
        from airflow.sdk.definitions.asset import Dataset
    except ImportError:
        from airflow.datasets import Dataset

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=Path(__file__).parent.parent.parent / "dev/dags/dbt/altered_jaffle_shop",
            task_id="run",
            dbt_cmd_flags=["--models", "ｍｕｌｔｉｂｙｔｅ"],
            install_deps=True,
            append_env=True,
        )
        run_operator

    run_test_dag(dag)

    assert run_operator.outlets == [
        Dataset(
            uri="postgres://0.0.0.0:5432/postgres/public/%EF%BD%8D%EF%BD%95%EF%BD%8C%EF%BD%94%EF%BD%89%EF%BD%82%EF%BD%99%EF%BD%94%EF%BD%85",
            extra=None,
        )
    ]


@pytest.mark.integration
def test_run_operator_caches_partial_parsing(caplog, tmp_path):
    caplog.clear()
    caplog.set_level(logging.DEBUG)
    with DAG("test-partial-parsing", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
            cache_dir=cache._obtain_cache_dir_path("test-partial-parsing", tmp_path),
            invocation_mode=InvocationMode.SUBPROCESS,
        )
        seed_operator

    run_test_dag(dag)

    # Unable to do partial parsing because saved manifest not found. Starting full parse.
    assert "Unable to do partial parsing" in caplog.text

    caplog.clear()
    run_test_dag(dag)

    assert not "Unable to do partial parsing" in caplog.text


@pytest.mark.integration
def test_run_operator_copies_manifest_file(caplog, tmp_path):
    manifest_filepath = DBT_PROJ_DIR / "target/manifest.json"
    assert manifest_filepath.exists()
    caplog.clear()
    caplog.set_level(logging.DEBUG)
    with DAG("test-partial-parsing", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
            cache_dir=cache._obtain_cache_dir_path("test-partial-parsing", tmp_path),
            invocation_mode=InvocationMode.SUBPROCESS,
            manifest_filepath=manifest_filepath,
        )
        seed_operator

    run_test_dag(dag)

    assert caplog.text.count("Copying the manifest from") == 1


def test_dbt_base_operator_no_partial_parse() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        partial_parse=False,
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )

    assert "--no-partial-parse" in cmd


@pytest.mark.integration
@pytest.mark.parametrize("invocation_mode", [InvocationMode.SUBPROCESS, InvocationMode.DBT_RUNNER])
def test_run_test_operator_with_callback(invocation_mode, failing_test_dbt_project):
    on_warning_callback = MagicMock()

    with DAG("test-id-2", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtSeedLocalOperator(
            profile_config=mini_profile_config,
            project_dir=failing_test_dbt_project,
            task_id="run",
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=mini_profile_config,
            project_dir=failing_test_dbt_project,
            task_id="test",
            append_env=True,
            on_warning_callback=on_warning_callback,
            invocation_mode=invocation_mode,
        )

        build_operator = DbtBuildLocalOperator(
            profile_config=mini_profile_config,
            project_dir=failing_test_dbt_project,
            task_id="build",
            append_env=True,
            on_warning_callback=on_warning_callback,
            invocation_mode=invocation_mode,
        )

        run_operator >> build_operator >> test_operator
    run_test_dag(dag)
    assert on_warning_callback.called


@pytest.mark.integration
@pytest.mark.parametrize("invocation_mode", [InvocationMode.SUBPROCESS, InvocationMode.DBT_RUNNER])
def test_run_test_operator_without_callback(invocation_mode):
    on_warning_callback = MagicMock()

    with DAG("test-id-3", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtSeedLocalOperator(
            profile_config=mini_profile_config,
            project_dir=MINI_DBT_PROJ_DIR,
            task_id="run",
            append_env=True,
            invocation_mode=invocation_mode,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=mini_profile_config,
            project_dir=MINI_DBT_PROJ_DIR,
            task_id="test",
            append_env=True,
            on_warning_callback=on_warning_callback,
            invocation_mode=invocation_mode,
        )
        run_operator >> test_operator
    run_test_dag(dag)
    assert not on_warning_callback.called


@pytest.mark.integration
def test_run_operator_emits_events():
    class MockRun:
        facets = {"c": 3}

    class MockJob:
        facets = {"d": 4}

    class MockEvent:
        inputs = [1]
        outputs = [2]
        run = MockRun()
        job = MockJob()

    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )
    dbt_base_operator.openlineage_events_completes = [MockEvent(), MockEvent()]
    facets = dbt_base_operator.get_openlineage_facets_on_complete(dbt_base_operator)
    assert facets.inputs == [1]
    assert facets.outputs == [2]
    assert facets.run_facets == {"c": 3}
    assert facets.job_facets == {"d": 4}


def test_run_operator_emits_events_without_openlineage_events_completes(caplog):
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )
    delattr(dbt_base_operator, "openlineage_events_completes")

    if version.parse(airflow_version) >= version.Version("3.1"):
        task_instance = TaskInstance(dbt_base_operator, dag_version_id=None)
    else:
        task_instance = TaskInstance(dbt_base_operator)

    facets = dbt_base_operator.get_openlineage_facets_on_complete(task_instance)

    assert facets.inputs == []
    assert facets.outputs == []
    assert facets.run_facets == {}
    assert facets.job_facets == {}
    assert "Unable to emit OpenLineage events due to lack of dependencies or data." in caplog.text


@pytest.mark.skipif(version.parse(airflow_version).major == 3, reason="Test only applies to Airflow 2")
def test_store_compiled_sql_airflow2() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )
    # here we just need to call the method to make sure it doesn't raise an exception
    dbt_base_operator.store_compiled_sql(
        tmp_project_dir="my/dir",
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )

    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=True,
    )
    dbt_base_operator.store_compiled_sql(
        tmp_project_dir="my/dir",
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    # here we call the method and see if it tries to access the context["ti"]
    # it should, and it should raise a KeyError because we didn't pass in a ti
    with pytest.raises(KeyError):
        dbt_base_operator._override_rtif(
            context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
        )


@pytest.mark.skipif(version.parse(airflow_version).major == 2, reason="Test only applies to Airflow 3")
def test_store_compiled_sql_airflow3() -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )
    # here we just need to call the method to make sure it doesn't raise an exception
    dbt_base_operator.store_compiled_sql(
        tmp_project_dir="my/dir",
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )

    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=True,
    )
    dbt_base_operator.store_compiled_sql(
        tmp_project_dir="my/dir",
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    # Test Airflow 3 behavior - should set flag
    dbt_base_operator._override_rtif(
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    assert dbt_base_operator.overwrite_rtif_after_execution is True


@pytest.mark.parametrize(
    "operator_class,kwargs,expected_call_kwargs",
    [
        (
            DbtSeedLocalOperator,
            {"full_refresh": True},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["seed", "--full-refresh"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtBuildLocalOperator,
            {"full_refresh": True},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["build", "--full-refresh"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtRunLocalOperator,
            {"full_refresh": True},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["run", "--full-refresh"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtCloneLocalOperator,
            {"full_refresh": True},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["clone", "--full-refresh"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtTestLocalOperator,
            {},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["test"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtTestLocalOperator,
            {"select": []},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["test"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "select": ["tag:daily"], "exclude": ["tag:disabled"]},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["test", "--select", "tag:daily", "--exclude", "tag:disabled"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "selector": "nightly_snowplow"},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["test", "--selector", "nightly_snowplow"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
        (
            DbtRunOperationLocalOperator,
            {"args": {"days": 7, "dry_run": True}, "macro_name": "bla"},
            {
                "context": {},
                "env": {},
                "cmd_flags": ["run-operation", "bla", "--args", "days: 7\ndry_run: true\n"],
                "run_as_async": False,
                "async_context": None,
                "push_run_results_to_xcom": False,
            },
        ),
    ],
)
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_command")
def test_operator_execute_with_flags(mock_run_cmd, operator_class, kwargs, expected_call_kwargs):
    task = operator_class(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.DBT_RUNNER,
        **kwargs,
    )
    task.get_env = MagicMock(return_value={})
    task.execute(context={})
    mock_run_cmd.assert_called_once_with(
        cmd=[task.dbt_executable_path, *expected_call_kwargs.pop("cmd_flags")], **expected_call_kwargs
    )


@pytest.mark.parametrize(
    "operator_class",
    (
        DbtLSLocalOperator,
        DbtSnapshotLocalOperator,
        DbtTestLocalOperator,
        DbtBuildLocalOperator,
        DbtDocsLocalOperator,
        DbtDocsS3LocalOperator,
        DbtDocsAzureStorageLocalOperator,
        DbtDocsGCSLocalOperator,
    ),
)
@patch("cosmos.operators.local.DbtLocalBaseOperator.build_and_run_cmd")
def test_operator_execute_without_flags(mock_build_and_run_cmd, operator_class):
    operator_class_kwargs = {
        DbtDocsS3LocalOperator: {"aws_conn_id": "fake-conn", "bucket_name": "fake-bucket"},
        DbtDocsAzureStorageLocalOperator: {"azure_conn_id": "fake-conn", "container_name": "fake-container"},
        DbtDocsGCSLocalOperator: {"connection_id": "fake-conn", "bucket_name": "fake-bucket"},
    }
    task = operator_class(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.DBT_RUNNER,
        **operator_class_kwargs.get(operator_class, {}),
    )
    task.execute(context={})
    mock_build_and_run_cmd.assert_called_once_with(context={}, cmd_flags=[])


@patch("cosmos.operators.local.DbtLocalArtifactProcessor")
def test_calculate_openlineage_events_completes_openlineage_errors(mock_processor, caplog):
    instance = mock_processor.return_value
    instance.parse = MagicMock(side_effect=KeyError)
    caplog.set_level(logging.DEBUG)
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir=DBT_PROJ_DIR,
        should_store_compiled_sql=False,
    )

    dbt_base_operator.calculate_openlineage_events_completes(env={}, project_dir=DBT_PROJ_DIR)

    assert instance.parse.called
    assert "Unable to parse OpenLineage events" in caplog.text


@pytest.mark.parametrize(
    "operator_class,expected_template",
    [
        (
            DbtSeedLocalOperator,
            (
                "env",
                "select",
                "exclude",
                "selector",
                "vars",
                "models",
                "dbt_cmd_flags",
                "compiled_sql",
                "freshness",
                "full_refresh",
            ),
        ),
        (
            DbtRunLocalOperator,
            (
                "env",
                "select",
                "exclude",
                "selector",
                "vars",
                "models",
                "dbt_cmd_flags",
                "compiled_sql",
                "freshness",
                "full_refresh",
            ),
        ),
        (
            DbtBuildLocalOperator,
            (
                "env",
                "select",
                "exclude",
                "selector",
                "vars",
                "models",
                "dbt_cmd_flags",
                "compiled_sql",
                "freshness",
                "full_refresh",
            ),
        ),
        (
            DbtSourceLocalOperator,
            ("env", "select", "exclude", "selector", "vars", "models", "dbt_cmd_flags", "compiled_sql", "freshness"),
        ),
    ],
)
def test_dbt_base_operator_template_fields(operator_class, expected_template):
    # Check if value of template fields is what we expect for the operators we're validating
    dbt_base_operator = operator_class(profile_config=profile_config, task_id="my-task", project_dir="my/dir")
    assert dbt_base_operator.template_fields == expected_template


@patch.object(DbtDocsGCSLocalOperator, "required_files", ["file1", "file2"])
def test_dbt_docs_gcs_local_operator():
    mock_gcs = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.google.cloud.hooks.gcs": mock_gcs}):
        operator = DbtDocsGCSLocalOperator(
            task_id="fake-task",
            project_dir="fake-dir",
            profile_config=profile_config,
            connection_id="fake-conn",
            bucket_name="fake-bucket",
            folder_dir="fake-folder",
        )
        operator.upload_to_cloud_storage("fake-dir")

        # assert that GCSHook was called with the connection id
        mock_gcs.GCSHook.assert_called_once_with("fake-conn")

        mock_hook = mock_gcs.GCSHook.return_value
        # assert that upload was called twice with the expected arguments
        assert mock_hook.upload.call_count == 2
        expected_upload_calls = [
            call(filename="fake-dir/target/file1", bucket_name="fake-bucket", object_name="fake-folder/file1"),
            call(filename="fake-dir/target/file2", bucket_name="fake-bucket", object_name="fake-folder/file2"),
        ]
        mock_hook.upload.assert_has_calls(expected_upload_calls)


@pytest.mark.integration
@patch("cosmos.operators.local.AbstractDbtLocalBase._upload_sql_files")
@patch("cosmos.operators.local.DbtLocalBaseOperator._override_rtif")
@patch("cosmos.operators.local.DbtLocalBaseOperator.handle_exception_subprocess")
@patch("cosmos.config.ProfileConfig.ensure_profile")
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_subprocess")
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_dbt_runner")
@patch("cosmos.operators.local.tempfile.TemporaryDirectory")
@pytest.mark.parametrize("invocation_mode", [InvocationMode.SUBPROCESS, InvocationMode.DBT_RUNNER])
def test_operator_execute_deps_parameters(
    mock_temporary_directory,
    mock_dbt_runner,
    mock_subprocess,
    mock_ensure_profile,
    mock_exception_handling,
    mock_override_rtif,
    mock_upload_sql_files,
    invocation_mode,
    tmp_path,
):
    project_dir = tmp_path / "mock_project_tmp_dir"
    project_dir.mkdir()

    expected_call_kwargs = [
        "/usr/local/bin/dbt",
        "deps",
        "--project-dir",
        project_dir.as_posix(),
        "--profiles-dir",
        "/path/to",
        "--profile",
        "default",
        "--target",
        "dev",
    ]
    task = DbtRunLocalOperator(
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        profile_config=real_profile_config,
        task_id="my-task",
        project_dir=DBT_PROJ_DIR,
        install_deps=True,
        emit_datasets=False,
        dbt_executable_path="/usr/local/bin/dbt",
        invocation_mode=invocation_mode,
    )
    mock_ensure_profile.return_value.__enter__.return_value = (Path("/path/to/profile"), {"ENV_VAR": "value"})
    mock_temporary_directory.return_value.__enter__.return_value = project_dir.as_posix()
    task.execute(context={"task_instance": MagicMock(), "run_id": "test_run_id"})
    if invocation_mode == InvocationMode.SUBPROCESS:
        assert mock_subprocess.call_args_list[0].kwargs["command"] == expected_call_kwargs
    elif invocation_mode == InvocationMode.DBT_RUNNER:
        mock_dbt_runner.all_args_list[0].kwargs["command"] == expected_call_kwargs


def test_dbt_docs_local_operator_with_static_flag():
    # Check when static flag is passed, the required files are correctly adjusted to a single file
    operator = DbtDocsLocalOperator(
        task_id="fake-task",
        project_dir="fake-dir",
        profile_config=profile_config,
        dbt_cmd_flags=["--static"],
    )
    assert operator.required_files == ["static_index.html"]


def test_dbt_docs_local_operator_ignores_graph_gpickle():
    # Check when --no-write-json is passed, graph.gpickle is removed.
    # This is only currently relevant for subclasses, but will become more generally relevant in the future.
    class CustomDbtDocsLocalOperator(DbtDocsLocalOperator):
        required_files = ["index.html", "manifest.json", "graph.gpickle", "catalog.json"]

    operator = CustomDbtDocsLocalOperator(
        task_id="fake-task",
        project_dir="fake-dir",
        profile_config=profile_config,
        dbt_cmd_global_flags=["--no-write-json"],
    )
    assert operator.required_files == ["index.html", "manifest.json", "catalog.json"]


@patch("cosmos.hooks.subprocess.FullOutputSubprocessHook.send_sigint")
def test_dbt_local_operator_on_kill_sigint(mock_send_sigint) -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        cancel_query_on_kill=True,
        invocation_mode=InvocationMode.SUBPROCESS,
    )

    dbt_base_operator.on_kill()

    mock_send_sigint.assert_called_once()


@patch("cosmos.hooks.subprocess.FullOutputSubprocessHook.send_sigterm")
def test_dbt_local_operator_on_kill_sigterm(mock_send_sigterm) -> None:
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        cancel_query_on_kill=False,
        invocation_mode=InvocationMode.SUBPROCESS,
    )

    dbt_base_operator.on_kill()

    mock_send_sigterm.assert_called_once()


def test_handle_exception_subprocess(caplog):
    """
    Test the handle_exception_subprocess method of the DbtLocalBaseOperator class for non-zero dbt exit code.
    """
    caplog.set_level(logging.ERROR)
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=None,
        task_id="my-task",
        project_dir="my/dir",
    )
    full_output = ["n" * n for n in range(1, 1000)]
    result = FullOutputSubprocessResult(exit_code=1, output="test", full_output=full_output)

    # Test when exit_code is non-zero
    with pytest.raises(AirflowException) as err_context:
        operator.handle_exception_subprocess(result)

    assert len(str(err_context.value)) < 100  # Ensure the error message is not too long
    assert "\n".join(full_output) in caplog.text


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_session():
    return MagicMock()


@patch("cosmos.operators.local.Path")
def test_store_freshness_json(mock_path_class, mock_context, mock_session):
    instance = DbtSourceLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
    )

    # Mock the behavior of Path.exists() and Path.read_text()
    mock_sources_json_path = MagicMock()
    mock_path_class.return_value = mock_sources_json_path
    mock_sources_json_path.exists.return_value = True
    mock_sources_json_path.read_text.return_value = '{"key": "value"}'

    # Expected formatted JSON content
    expected_freshness = json.dumps({"key": "value"}, indent=4)

    # Call the method under test
    instance.store_freshness_json(tmp_project_dir="/mock/dir", context=mock_context)

    # Verify the freshness attribute is set correctly
    assert instance.freshness == expected_freshness


@patch("cosmos.operators.local.Path")
def test_store_freshness_json_no_file(mock_path_class, mock_context, mock_session):
    # Create an instance of the class that contains the method
    instance = DbtSourceLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
    )

    # Mock the behavior of Path.exists() and Path.read_text()
    mock_sources_json_path = MagicMock()
    mock_path_class.return_value = mock_sources_json_path
    mock_sources_json_path.exists.return_value = False

    # Call the method under test
    instance.store_freshness_json(tmp_project_dir="/mock/dir", context=mock_context)

    # Verify the freshness attribute is set correctly
    assert instance.freshness == ""


def test_store_freshness_not_store_compiled_sql(mock_context, mock_session):
    instance = DbtSourceLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    # Call the method under test
    instance.store_freshness_json(tmp_project_dir="/mock/dir", context=mock_context)

    # Verify the freshness attribute is set correctly
    assert instance.freshness == ""


@pytest.mark.parametrize(
    "invocation_mode, expected_extract_function",
    [
        (InvocationMode.SUBPROCESS, "cosmos.operators.local.extract_freshness_warn_msg"),
        (InvocationMode.DBT_RUNNER, "cosmos.dbt.runner.extract_message_by_status"),
    ],
)
def test_handle_warnings(invocation_mode, expected_extract_function, mock_context):
    result = MagicMock()

    instance = DbtSourceLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
        on_warning_callback=lambda context: print(context),
        invocation_mode=invocation_mode,
    )

    with (
        patch(expected_extract_function) as mock_extract_issues,
        patch.object(instance, "on_warning_callback") as mock_on_warning_callback,
    ):
        mock_extract_issues.return_value = (["test_name1", "test_name2"], ["test_name1", "test_name2"])

        instance._handle_warnings(result, mock_context)

        mock_extract_issues.assert_called_once_with(result)

        mock_on_warning_callback.assert_called_once_with(
            {**mock_context, "test_names": ["test_name1", "test_name2"], "test_results": ["test_name1", "test_name2"]}
        )


def test_dbt_compile_local_operator_initialisation():
    operator = DbtCompileLocalOperator(
        task_id="fake-task",
        profile_config=profile_config,
        project_dir="fake-dir",
    )
    assert operator.should_upload_compiled_sql is True
    assert "compile" in operator.base_cmd


@patch("cosmos.operators.local.DbtLocalBaseOperator._override_rtif")
@patch("cosmos.operators.local.AbstractDbtLocalBase._upload_sql_files")
@patch("cosmos.operators.local.AbstractDbtLocalBase.store_compiled_sql")
def test_dbt_compile_local_operator_execute(store_compiled_sql, _upload_sql_files, mock_override_rtif):
    operator = DbtCompileLocalOperator(
        task_id="fake-task",
        profile_config=profile_config,
        project_dir="fake-dir",
    )

    operator._handle_post_execution("fake-dir", {})

    assert operator.should_upload_compiled_sql is True
    _upload_sql_files.assert_called_once()
    store_compiled_sql.assert_called_once()
    mock_override_rtif.assert_called_once()


def test_dbt_clone_local_operator_initialisation():
    operator = DbtCloneLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="clone",
        dbt_cmd_flags=["--state", "/usr/local/airflow/dbt/jaffle_shop/target"],
        install_deps=True,
        append_env=True,
    )

    assert "clone" in operator.base_cmd


@pytest.mark.parametrize(
    "rem_target_path, rem_target_path_conn_id",
    [
        (None, "aws_s3_conn"),
        ("unknown://some-bucket/cache", None),
    ],
)
def test_config_remote_target_path_unset_settings(rem_target_path, rem_target_path_conn_id):
    with patch("cosmos.operators.local.remote_target_path", new=rem_target_path):
        with patch("cosmos.operators.local.remote_target_path_conn_id", new=rem_target_path_conn_id):
            operator = DbtCompileLocalOperator(
                task_id="fake-task",
                profile_config=profile_config,
                project_dir="fake-dir",
            )
            target_path, target_conn = operator._configure_remote_target_path()
        assert target_path is None
        assert target_conn is None


@patch("cosmos.operators.local.remote_target_path", new="s3://some-bucket/target")
@patch("cosmos.operators.local.remote_target_path_conn_id", new="aws_s3_conn")
@patch("cosmos.operators.local.ObjectStoragePath")
def test_configure_remote_target_path(mock_object_storage_path):
    operator = DbtCompileLocalOperator(
        task_id="fake-task",
        profile_config=profile_config,
        project_dir="fake-dir",
    )
    mock_remote_path = MagicMock()
    mock_object_storage_path.return_value.exists.return_value = True
    mock_object_storage_path.return_value = mock_remote_path
    target_path, target_conn = operator._configure_remote_target_path()
    assert target_path == mock_remote_path
    assert target_conn == "aws_s3_conn"
    mock_object_storage_path.assert_called_with("s3://some-bucket/target", conn_id="aws_s3_conn")

    mock_object_storage_path.return_value.exists.return_value = False
    mock_object_storage_path.return_value.mkdir.return_value = MagicMock()
    _, _ = operator._configure_remote_target_path()
    mock_object_storage_path.return_value.mkdir.assert_called_with(parents=True, exist_ok=True)


@patch("cosmos.operators.local.DbtCompileLocalOperator._configure_remote_target_path")
def test_upload_compiled_sql_no_remote_path_raises_error(mock_configure_remote):
    operator = DbtCompileLocalOperator(
        task_id="fake-task",
        profile_config=profile_config,
        project_dir="fake-dir",
    )

    mock_configure_remote.return_value = (None, None)

    tmp_project_dir = "/fake/tmp/project"

    with pytest.raises(CosmosValueError, match="remote target path is not configured"):
        operator._upload_sql_files(tmp_project_dir, "compiled")


def test_upload_sql_files_xcom(tmp_path):
    sql_query = "SELECT 1;"
    sql_file = tmp_path / "target" / "models" / "dest.sql"
    sql_file.parent.mkdir(parents=True, exist_ok=True)
    sql_file.write_text(sql_query)

    mock_context = {"ti": MagicMock()}

    obj = DbtRunLocalOperator(
        task_id="test",
        project_dir="/tmp",
        profile_config=profile_config,
    )
    obj._construct_dest_file_path = lambda a, b, c, d: "dest.sql"

    obj._upload_sql_files_xcom(mock_context, str(tmp_path), "models")

    compressed_sql = zlib.compress(sql_query.encode("utf-8"))
    compressed_b64_sql = base64.b64encode(compressed_sql).decode("utf-8")
    mock_context["ti"].xcom_push.assert_called_once_with(key="dest.sql", value=compressed_b64_sql)


@patch("cosmos.settings.upload_sql_to_xcom", False)
@patch("cosmos.operators.local.ObjectStoragePath.copy")
@patch("cosmos.operators.local.ObjectStoragePath")
@patch("cosmos.operators.local.DbtCompileLocalOperator._configure_remote_target_path")
def test_upload_compiled_sql_should_upload(mock_configure_remote, mock_object_storage_path, mock_copy):
    """Test upload_compiled_sql when should_upload_compiled_sql is True and uploads files."""
    operator = DbtCompileLocalOperator(
        task_id="fake-task",
        profile_config=profile_config,
        project_dir="fake-dir",
        dag=DAG("test_dag", start_date=datetime(2024, 4, 16)),
        extra_context={"dbt_dag_task_group_identifier": "test_dag"},
    )

    mock_configure_remote.return_value = ("mock_remote_path", "mock_conn_id")

    tmp_project_dir = "/fake/tmp/project"
    source_compiled_dir = Path(tmp_project_dir) / "target" / "compiled"

    file1 = MagicMock(spec=Path)
    file1.is_file.return_value = True
    file1.__str__.return_value = str(source_compiled_dir / "file1.sql")

    file2 = MagicMock(spec=Path)
    file2.is_file.return_value = True
    file2.__str__.return_value = str(source_compiled_dir / "file2.sql")

    files = [file1, file2]

    operator.extra_context["run_id"] = "test_run_id"
    with patch.object(Path, "rglob", return_value=files):
        operator._upload_sql_files(tmp_project_dir, "compiled")

        for file_path in files:
            rel_path = os.path.relpath(str(file_path), str(source_compiled_dir))
            expected_dest_path = f"mock_remote_path/test_dag/test_run_id/compiled/{rel_path.lstrip('/')}"
            mock_object_storage_path.assert_any_call(expected_dest_path, conn_id="mock_conn_id")
            mock_object_storage_path.return_value.copy.assert_any_call(mock_object_storage_path.return_value)


def test_mock_dbt_adapter_valid_context():
    """
    Test that the _mock_dbt_adapter method calls the correct mock adapter function
    when provided with a valid async_context.
    """
    async_context = {
        "async_operator": MagicMock(),
        "profile_type": "bigquery",
    }
    AbstractDbtLocalBase.__abstractmethods__ = set()
    operator = AbstractDbtLocalBase(task_id="test_task", project_dir="test_project", profile_config=MagicMock())
    with patch("cosmos.operators.local.load_method_from_module") as mock_load_method:
        operator._mock_dbt_adapter(async_context)

    expected_module_path = "cosmos.operators._asynchronous.bigquery"
    expected_method_name = "_mock_bigquery_adapter"
    mock_load_method.assert_called_once_with(expected_module_path, expected_method_name)


def test_mock_dbt_adapter_missing_async_context():
    """
    Test that the _mock_dbt_adapter method raises a CosmosValueError
    when async_context is None.
    """
    AbstractDbtLocalBase.__abstractmethods__ = set()
    operator = AbstractDbtLocalBase(task_id="test_task", project_dir="test_project", profile_config=MagicMock())
    with pytest.raises(CosmosValueError, match="`async_context` is necessary for running the model asynchronously"):
        operator._mock_dbt_adapter(None)


def test_mock_dbt_adapter_missing_profile_type():
    """
    Test that the _mock_dbt_adapter method raises a CosmosValueError
    when profile_type is missing in async_context.
    """
    async_context = {
        "async_operator": MagicMock(),
    }
    AbstractDbtLocalBase.__abstractmethods__ = set()
    operator = AbstractDbtLocalBase(task_id="test_task", project_dir="test_project", profile_config=MagicMock())
    with pytest.raises(CosmosValueError, match="`profile_type` needs to be specified in `async_context`"):
        operator._mock_dbt_adapter(async_context)


def test_mock_dbt_adapter_unsupported_profile_type():
    """
    Test that the _mock_dbt_adapter method raises a CosmosValueError
    when the profile_type is not supported.
    """
    async_context = {
        "async_operator": MagicMock(),
        "profile_type": "unsupported_profile",
    }
    AbstractDbtLocalBase.__abstractmethods__ = set()
    operator = AbstractDbtLocalBase(task_id="test_task", project_dir="test_project", profile_config=MagicMock())
    with pytest.raises(
        ModuleNotFoundError,
        match="Module cosmos.operators._asynchronous.unsupported_profile not found",
    ):
        operator._mock_dbt_adapter(async_context)


@patch("airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator.execute")
@patch("cosmos.operators.local.AbstractDbtLocalBase._read_run_sql_from_target_dir")
@patch("cosmos.operators.local.settings.enable_setup_async_task", False)
def test_async_execution_without_start_task(mock_read_sql, mock_bq_execute):
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

    mock_read_sql.return_value = "select * from 1;"
    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir="/tmp",
        profile_config=profile_config,
    )
    operator._handle_async_execution(
        "/tmp", {}, {"profile_type": "bigquery", "async_operator": BigQueryInsertJobOperator}
    )
    mock_bq_execute.assert_called_once()


def test_build_and_run_cmd_with_full_refresh_in_async_mode():
    """Test that build_and_run_cmd adds --full-refresh flag when full_refresh is True in async mode."""
    AbstractDbtLocalBase.__abstractmethods__ = set()

    with patch("cosmos.operators.local.settings.enable_setup_async_task", True):
        operator = AbstractDbtLocalBase(
            task_id="test_task",
            project_dir="/tmp",
            profile_config=profile_config,
        )
        operator.full_refresh = True

        with patch.object(operator, "build_cmd") as mock_build_cmd:
            mock_build_cmd.return_value = (["dbt", "run"], {})

            with patch.object(operator, "run_command"):
                operator.build_and_run_cmd(context={}, run_as_async=True)
                cmd_flags_arg = mock_build_cmd.call_args[1].get("cmd_flags", [])
                assert "--full-refresh" in cmd_flags_arg


def test_read_run_sql_from_target_dir():
    tmp_project_dir = "/tmp/project"
    sql_context = {"dbt_node_config": {"file_path": "/path/to/file.sql"}, "package_name": "package_name"}

    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir="/tmp",
        profile_config=profile_config,
    )

    expected_sql_content = "SELECT * FROM my_table;"
    with patch("pathlib.Path.open", new_callable=mock_open, read_data=expected_sql_content):
        result = operator._read_run_sql_from_target_dir(tmp_project_dir, sql_context)
        assert result == expected_sql_content


@patch("cosmos.operators.local.copy_dbt_packages")
@patch("cosmos.operators.local.create_symlinks")
def test_test_clone_project(create_symlinks_mock, copy_dbt_packages_mock, caplog):
    project_dir = Path("/fake/project")
    tmp_dir_path = Path("/fake/tmp")
    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir=project_dir,
        install_deps=False,
        copy_dbt_packages=True,
        profile_config=profile_config,
    )
    operator._clone_project(tmp_dir_path)

    create_symlinks_mock.assert_called_once_with(project_dir, tmp_dir_path, ignore_dbt_packages=True)
    copy_dbt_packages_mock.assert_called_once_with(project_dir, tmp_dir_path)

    assert f"Cloning project to writable temp directory {tmp_dir_path} from {project_dir}" in caplog.text
    assert "Copying dbt packages to temporary folder." in caplog.text
    assert "Completed copying dbt packages to temporary folder." in caplog.text


@patch("cosmos.operators.local.AbstractDbtLocalBase.store_freshness_json")
@patch("cosmos.operators.local.AbstractDbtLocalBase.store_compiled_sql")
@patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
def test_handle_post_execution_with_multiple_callbacks(
    mock_override_rtif, mock_store_compiled_sql, mock_store_freshness_json
):
    multiple_callbacks = [MagicMock(), MagicMock(), MagicMock()]
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        callback=multiple_callbacks,
        callback_args={"arg1": "value1"},
    )

    context = {"dag_run": MagicMock(), "task": MagicMock()}
    operator._handle_post_execution("/tmp/project_dir", context)

    for callback_fn in multiple_callbacks:
        callback_fn.assert_called_once_with("/tmp/project_dir", arg1="value1", context=context)


@pytest.mark.integration
@patch("cosmos.operators.local.AbstractDbtLocalBase._configure_remote_target_path")
@patch("cosmos.operators.local.ObjectStoragePath")
def test_delete_sql_files_directory_not_exists(mock_object_storage_path, mock_configure_remote, caplog):
    """Test the _delete_sql_files method when the remote directory doesn't exist."""
    caplog.set_level(logging.DEBUG)
    mock_path = MagicMock()
    mock_path.exists.return_value = False
    mock_object_storage_path.return_value = mock_path
    mock_configure_remote.return_value = (Path("/mock/path"), "mock_conn_id")

    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir="/project/dir",
        profile_config=profile_config,
        extra_context={"dbt_dag_task_group_identifier": "test_dag_task_group", "run_id": "test_run_id"},
    )
    operator._delete_sql_files()
    assert (
        "Remote run directory does not exist, skipping deletion: /mock/path/test_dag_task_group/test_run_id"
        in caplog.text
    )

    mock_path.rmdir.assert_not_called()


@pytest.mark.integration
def test_generate_dbt_flags_appends_no_static_parser(tmp_path):
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="test-task",
        project_dir=tmp_path,
    )
    operator.invocation_mode = InvocationMode.DBT_RUNNER
    tmp_project_dir = str(tmp_path)
    profile_path = tmp_path / "profiles.yml"
    flags = operator._generate_dbt_flags(tmp_project_dir, profile_path)
    assert "--no-static-parser" in flags


def test_generate_dbt_flags_does_not_append_no_static_parser_in_subprocess(tmp_path):
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="test-task",
        project_dir=tmp_path,
    )
    operator.invocation_mode = InvocationMode.SUBPROCESS
    tmp_project_dir = str(tmp_path)
    profile_path = tmp_path / "profiles.yml"
    flags = operator._generate_dbt_flags(tmp_project_dir, profile_path)
    assert "--no-static-parser" not in flags


@pytest.mark.integration
@patch("cosmos.operators.local.AbstractDbtLocalBase._configure_remote_target_path")
def test_delete_sql_files_no_remote_target_configured(mock_configure_remote, caplog):
    """Test that _delete_sql_files exits early with a warning when remote path is not configured."""
    caplog.set_level(logging.WARNING)
    mock_configure_remote.return_value = (None, None)
    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir="/project/dir",
        profile_config=profile_config,
        extra_context={"dbt_dag_task_group_identifier": "test_dag_task_group", "run_id": "test_run_id"},
    )

    operator._delete_sql_files()
    expected_log_message = "Remote target path or connection ID not configured. Skipping deletion."
    assert expected_log_message in caplog.text

    mock_configure_remote.return_value = (Path("/mock/path"), None)
    operator._delete_sql_files()
    assert expected_log_message in caplog.text


@pytest.mark.skipif(version.parse(airflow_version).major == 3, reason="Test only applies to Airflow 2")
def test_override_rtif_airflow2_with_should_store_compiled_sql_false():
    """Test that _override_rtif does nothing in Airflow 2 when should_store_compiled_sql is False"""
    operator = DbtRunLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    context = {"task_instance": MagicMock()}

    operator._override_rtif(context)
    assert not context["task_instance"].called


@pytest.mark.skipif(version.parse(airflow_version).major == 2, reason="Test only applies to Airflow 3")
def test_override_rtif_airflow3_with_should_store_compiled_sql_false():
    """Test that _override_rtif sets overwrite_rtif_after_execution to False in Airflow 3 when should_store_compiled_sql is False"""
    operator = DbtRunLocalOperator(
        task_id="test",
        profile_config=None,
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    context = {"task_instance": MagicMock()}
    operator._override_rtif(context)
    assert operator.overwrite_rtif_after_execution is False


@pytest.mark.skipif(version.parse(airflow_version).major == 3, reason="Test only applies to Airflow 2")
def test_override_rtif_airflow2_filters_by_map_index():
    """Test that _override_rtif correctly filters by map_index for dynamically mapped tasks in Airflow 2.

    This test ensures that when deleting old RenderedTaskInstanceFields records,
    the map_index filter is applied so that only the current mapped task instance's
    records are deleted, not records from other mapped task instances.

    This addresses issue #2018 where SQL templated fields weren't properly rendered
    for dynamically mapped tasks.
    """
    from airflow.models.renderedtifields import RenderedTaskInstanceFields

    with DAG("test_dag", start_date=datetime(2023, 1, 1)) as dag:
        operator = DbtRunLocalOperator(
            task_id="test",
            profile_config=profile_config,
            project_dir="my/dir",
            should_store_compiled_sql=True,
            dag=dag,
        )

    mock_ti = MagicMock(spec=TaskInstance)
    mock_ti.dag_id = "test_dag"
    mock_ti.task_id = "test"
    mock_ti.run_id = "test_run_1"
    mock_ti.map_index = 2  # Simulating a mapped task instance
    mock_ti.task = operator

    context = {"ti": mock_ti}

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_filter = MagicMock()

    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_filter
    mock_filter.delete.return_value = None

    with patch("airflow.utils.session.provide_session") as mock_provide_session:

        def session_decorator(func):
            def wrapper(*args, **kwargs):
                return func(session=mock_session)

            return wrapper

        mock_provide_session.side_effect = session_decorator

        operator._override_rtif(context)

        mock_session.query.assert_called_once_with(RenderedTaskInstanceFields)
        mock_query.filter.assert_called_once()
        filter_call_arg_map_index = str(mock_query.filter.call_args.args[-1])
        assert filter_call_arg_map_index == "rendered_task_instance_fields.map_index = :map_index_1"


def test_dbt_cmd_flags_templating():
    """Test that dbt_cmd_flags supports Jinja templating."""
    from datetime import datetime

    from airflow import DAG

    dag = DAG("test_dag", start_date=datetime(2023, 1, 1))

    operator = DbtRunLocalOperator(
        task_id="test_templating",
        project_dir="my/dir",
        profile_config=profile_config,
        dbt_cmd_flags=[
            "--warn-error",
            "{% if params.event_time_start %}--event-time-start{% endif %}",
            "{% if params.event_time_start %}{{ params.event_time_start }}{% endif %}",
            "{% if params.event_time_end %}--event-time-end{% endif %}",
            "{% if params.event_time_end %}{{ params.event_time_end }}{% endif %}",
            "--select",
            "{{ params.model_name if params.model_name else 'default_model' }}",
        ],
        dag=dag,
    )

    # Test with parameters
    context = {
        "params": {"event_time_start": "2024-01-01", "event_time_end": "2024-01-31", "model_name": "stg_funnel_events"}
    }

    # Render templates
    operator.render_template_fields(context)

    # Verify the flags were templated correctly
    expected_flags = [
        "--warn-error",
        "--event-time-start",
        "2024-01-01",
        "--event-time-end",
        "2024-01-31",
        "--select",
        "stg_funnel_events",
    ]

    assert operator.dbt_cmd_flags == expected_flags


def test_dbt_cmd_flags_empty_template():
    """Test that empty template results are filtered out."""
    from datetime import datetime

    from airflow import DAG

    dag = DAG("test_dag", start_date=datetime(2023, 1, 1))

    operator = DbtRunLocalOperator(
        task_id="test_empty",
        project_dir="my/dir",
        profile_config=profile_config,
        dbt_cmd_flags=[
            "--warn-error",
            "{% if params.get('missing_param') %}--some-flag{% endif %}",  # Will be empty
            "{% if params.get('missing_param') %}{{ params.get('missing_param') }}{% endif %}",  # Will be empty
            "--select",
            "{{ params.model_name if params.model_name else 'default_model' }}",
        ],
        dag=dag,
    )

    context = {"params": {"model_name": "test_model"}}  # No missing_param

    # Render templates
    operator.render_template_fields(context)

    # Build command to test filtering
    dbt_cmd, _ = operator.build_cmd(context, [])

    # Verify empty flags are filtered out and only non-empty flags remain
    # The command should contain the non-empty flags but not the empty ones
    cmd_str = " ".join(dbt_cmd)
    assert "--warn-error" in cmd_str
    assert "--select" in cmd_str
    assert "test_model" in cmd_str
    assert "--some-flag" not in cmd_str  # Empty template should be filtered out


def test_dbt_cmd_flags_mixed_static_and_templated():
    """Test that dbt_cmd_flags works with a mix of static and templated values."""
    from datetime import datetime

    from airflow import DAG

    dag = DAG("test_dag", start_date=datetime(2023, 1, 1))

    operator = DbtRunLocalOperator(
        task_id="test_mixed",
        project_dir="my/dir",
        profile_config=profile_config,
        dbt_cmd_flags=[
            "--full-refresh",  # Static flag
            "--select",
            "{{ params.model_name }}",  # Templated value
            "{% if params.use_cache %}--cache{% endif %}",  # Conditional templated flag
            "--threads",
            "4",  # Static value
        ],
        dag=dag,
    )

    context = {"params": {"model_name": "my_model", "use_cache": True}}

    # Render templates
    operator.render_template_fields(context)

    expected_flags = ["--full-refresh", "--select", "my_model", "--cache", "--threads", "4"]

    assert operator.dbt_cmd_flags == expected_flags


def test_push_run_results_to_xcom_missing_file():
    """Test that _push_run_results_to_xcom raises AirflowException when run_results.json doesn't exist."""
    from airflow.exceptions import AirflowException

    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir="/tmp",
        profile_config=profile_config,
    )
    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti}

    with pytest.raises(AirflowException) as exc_info:
        operator._push_run_results_to_xcom("/tmp", mock_context)
    assert "run_results.json not found" in str(exc_info.value)


def test_push_run_results_to_xcom_invalid_json(tmp_path):
    """Test that _push_run_results_to_xcom raises AirflowException when run_results.json is invalid JSON."""
    from airflow.exceptions import AirflowException

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    run_results_path = target_dir / "run_results.json"
    run_results_path.write_text("invalid json{")

    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir=str(tmp_path),
        profile_config=profile_config,
    )
    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti}

    with pytest.raises(AirflowException) as exc_info:
        operator._push_run_results_to_xcom(str(tmp_path), mock_context)
    assert "Invalid JSON in run_results.json" in str(exc_info.value)


def test_push_run_results_to_xcom_success(tmp_path):
    """Test that _push_run_results_to_xcom successfully pushes valid JSON to XCom."""
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    run_results_path = target_dir / "run_results.json"
    test_data = {"results": [{"status": "success"}]}
    run_results_path.write_text(json.dumps(test_data))

    operator = DbtRunLocalOperator(
        task_id="test",
        project_dir=str(tmp_path),
        profile_config=profile_config,
    )
    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti}

    operator._push_run_results_to_xcom(str(tmp_path), mock_context)

    mock_ti.xcom_push.assert_called_once()
    key, value = mock_ti.xcom_push.call_args[1]["key"], mock_ti.xcom_push.call_args[1]["value"]
    assert key == "run_results"

    decompressed = json.loads(zlib.decompress(base64.b64decode(value)).decode())
    assert decompressed == test_data


def test_dbt_cmd_flags_all_templated():
    """Test that dbt_cmd_flags works when all values are templated."""
    from datetime import datetime

    from airflow import DAG

    dag = DAG("test_dag", start_date=datetime(2023, 1, 1))

    operator = DbtRunLocalOperator(
        task_id="test_all_templated",
        project_dir="my/dir",
        profile_config=profile_config,
        dbt_cmd_flags=[
            "{{ params.flag1 }}",
            "{{ params.flag2 }}",
            "{{ params.value1 }}",
            "{{ params.value2 }}",
        ],
        dag=dag,
    )

    context = {"params": {"flag1": "--select", "flag2": "--exclude", "value1": "model1", "value2": "model2"}}

    # Render templates
    operator.render_template_fields(context)

    expected_flags = ["--select", "--exclude", "model1", "model2"]

    assert operator.dbt_cmd_flags == expected_flags


@patch("cosmos.settings.enable_uri_xcom", False)
def test_handle_datasets_does_not_push_uri_xcom_when_disabled():
    """Test that _handle_datasets does not push URI to XCom when enable_uri_xcom is False (default)."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    # Create mock Asset objects with uri attribute
    mock_outlet = MagicMock()
    mock_outlet.uri = "postgres://0.0.0.0:5432/postgres.public.test_table"

    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti, "outlet_events": MagicMock()}

    # Mock get_datasets to return mock assets and register_dataset to avoid database interactions
    with (
        patch.object(operator, "get_datasets", side_effect=[[], [mock_outlet]]),
        patch.object(operator, "register_dataset"),
    ):
        operator._handle_datasets(mock_context)

    # Verify xcom_push was NOT called with "uri" key
    uri_xcom_calls = [call for call in mock_ti.xcom_push.call_args_list if call[1].get("key") == "uri"]
    assert len(uri_xcom_calls) == 0, "URI XCom should not be pushed when enable_uri_xcom is False"


@patch("cosmos.settings.enable_uri_xcom", True)
def test_handle_datasets_pushes_uri_xcom_when_enabled():
    """Test that _handle_datasets pushes URI to XCom when enable_uri_xcom is True."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    # Create mock Asset objects with uri attribute
    mock_outlet = MagicMock()
    mock_outlet.uri = "postgres://0.0.0.0:5432/postgres.public.test_table"

    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti, "outlet_events": MagicMock()}

    # Mock get_datasets to return mock assets and register_dataset to avoid database interactions
    with (
        patch.object(operator, "get_datasets", side_effect=[[], [mock_outlet]]),
        patch.object(operator, "register_dataset"),
    ):
        operator._handle_datasets(mock_context)

    # Verify xcom_push was called with "uri" key and the correct value
    mock_ti.xcom_push.assert_called_once()
    call_kwargs = mock_ti.xcom_push.call_args[1]
    assert call_kwargs["key"] == "uri"
    assert isinstance(call_kwargs["value"], list)
    assert len(call_kwargs["value"]) == 1
    assert call_kwargs["value"][0] == "postgres://0.0.0.0:5432/postgres.public.test_table"


@patch("cosmos.settings.enable_uri_xcom", True)
def test_handle_datasets_pushes_multiple_uris_to_xcom():
    """Test that _handle_datasets pushes multiple URIs to XCom when there are multiple outlets."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    # Create mock Asset objects with uri attribute
    mock_outlet1 = MagicMock()
    mock_outlet1.uri = "postgres://0.0.0.0:5432/postgres.public.table1"
    mock_outlet2 = MagicMock()
    mock_outlet2.uri = "postgres://0.0.0.0:5432/postgres.public.table2"

    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti, "outlet_events": MagicMock()}

    # Mock get_datasets to return mock assets and register_dataset to avoid database interactions
    with (
        patch.object(operator, "get_datasets", side_effect=[[], [mock_outlet1, mock_outlet2]]),
        patch.object(operator, "register_dataset"),
    ):
        operator._handle_datasets(mock_context)

    # Verify xcom_push was called with correct URIs
    mock_ti.xcom_push.assert_called_once()
    call_kwargs = mock_ti.xcom_push.call_args[1]
    assert call_kwargs["key"] == "uri"
    assert len(call_kwargs["value"]) == 2
    assert "postgres://0.0.0.0:5432/postgres.public.table1" in call_kwargs["value"]
    assert "postgres://0.0.0.0:5432/postgres.public.table2" in call_kwargs["value"]


@patch("cosmos.settings.enable_uri_xcom", True)
def test_handle_datasets_does_not_push_xcom_when_no_outlets():
    """Test that _handle_datasets does not push XCom when there are no outlets."""
    operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        should_store_compiled_sql=False,
    )

    mock_ti = MagicMock()
    mock_context = {"ti": mock_ti, "outlet_events": MagicMock()}

    # Mock get_datasets to return empty lists and register_dataset to avoid database interactions
    with (
        patch.object(operator, "get_datasets", side_effect=[[], []]),
        patch.object(operator, "register_dataset"),
    ):
        operator._handle_datasets(mock_context)

    # Verify xcom_push was NOT called (no outlets to push)
    uri_xcom_calls = [call for call in mock_ti.xcom_push.call_args_list if call[1].get("key") == "uri"]
    assert len(uri_xcom_calls) == 0, "URI XCom should not be pushed when there are no outlets"
