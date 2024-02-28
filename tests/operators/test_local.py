import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow import DAG
from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from packaging import version
from pendulum import datetime

from cosmos.config import ProfileConfig
from cosmos.constants import InvocationMode
from cosmos.dbt.parser.output import (
    extract_dbt_runner_issues,
    parse_number_of_warnings_dbt_runner,
    parse_number_of_warnings_subprocess,
)
from cosmos.operators.local import (
    DbtBuildLocalOperator,
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
    DbtTestLocalOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
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
        conn_id="airflow_db",
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
    )
    assert dbt_base_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


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
    dbt_base_operator._set_invocation_methods()
    assert dbt_base_operator.invoke_dbt.__name__ == invoke_dbt_method
    assert dbt_base_operator.handle_exception.__name__ == handle_exception_method


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
        with pytest.raises(ImportError, match=expected_error_message):
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


@patch("cosmos.dbt.project.os.chdir")
def test_dbt_base_operator_run_dbt_runner_is_cached(mock_chdir):
    """Tests that if run_dbt_runner is called multiple times a cached runner is used."""
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        invocation_mode=InvocationMode.DBT_RUNNER,
    )
    mock_dbt = MagicMock()
    with patch.dict(sys.modules, {"dbt.cli.main": mock_dbt}):
        for _ in range(3):
            dbt_base_operator.run_dbt_runner(command=["cmd"], env={}, cwd="some-project")
    mock_dbt_runner = mock_dbt.dbtRunner
    assert mock_dbt_runner.call_count == 1
    assert dbt_base_operator._dbt_runner is not None


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

    with pytest.raises(AirflowException, match=expected_error_message):
        operator.handle_exception_dbt_runner(result)


@patch("cosmos.operators.local.extract_dbt_runner_issues", return_value=(["node1", "node2"], ["error1", "error2"]))
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

    with pytest.raises(AirflowException, match=expected_error_message):
        operator.handle_exception_dbt_runner(result)

    mock_extract_dbt_runner_issues.assert_called_once()


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_base_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
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
    assert operator.extract_issues == extract_dbt_runner_issues
    assert operator.parse_number_of_warnings == parse_number_of_warnings_dbt_runner


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.4"),
    reason="Airflow DAG did not have datasets until the 2.4 release",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets():
    from airflow.datasets import Dataset

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        seed_operator = DbtSeedLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="seed",
            dbt_cmd_flags=["--select", "raw_customers"],
            install_deps=True,
            append_env=True,
        )
        run_operator = DbtRunLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
            append_env=True,
        )
        seed_operator >> run_operator >> test_operator
    run_test_dag(dag)
    assert run_operator.inlets == []
    assert run_operator.outlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.inlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.outlets == []


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
        run_operator >> test_operator
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
    facets = dbt_base_operator.get_openlineage_facets_on_complete(dbt_base_operator)
    assert facets.inputs == []
    assert facets.outputs == []
    assert facets.run_facets == {}
    assert facets.job_facets == {}
    log = "Unable to emit OpenLineage events due to lack of dependencies or data."
    assert log in caplog.text


def test_store_compiled_sql() -> None:
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

    # here we call the method and see if it tries to access the context["ti"]
    # it should, and it should raise a KeyError because we didn't pass in a ti
    with pytest.raises(KeyError):
        dbt_base_operator.store_compiled_sql(
            tmp_project_dir="my/dir",
            context=Context(execution_date=datetime(2023, 2, 15, 12, 30)),
        )


@pytest.mark.parametrize(
    "operator_class,kwargs,expected_call_kwargs",
    [
        (
            DbtSeedLocalOperator,
            {"full_refresh": True},
            {"context": {}, "env": {}, "cmd_flags": ["seed", "--full-refresh"]},
        ),
        (
            DbtRunLocalOperator,
            {"full_refresh": True},
            {"context": {}, "env": {}, "cmd_flags": ["run", "--full-refresh"]},
        ),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "select": ["tag:daily"], "exclude": ["tag:disabled"]},
            {"context": {}, "env": {}, "cmd_flags": ["test", "--select", "tag:daily", "--exclude", "tag:disabled"]},
        ),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "selector": "nightly_snowplow"},
            {"context": {}, "env": {}, "cmd_flags": ["test", "--selector", "nightly_snowplow"]},
        ),
        (
            DbtRunOperationLocalOperator,
            {"args": {"days": 7, "dry_run": True}, "macro_name": "bla"},
            {"context": {}, "env": {}, "cmd_flags": ["run-operation", "bla", "--args", "days: 7\ndry_run: true\n"]},
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
    err_msg = "Unable to parse OpenLineage events"
    assert err_msg in caplog.text


@pytest.mark.parametrize(
    "operator_class,expected_template",
    [
        (DbtSeedLocalOperator, ("env", "vars", "compiled_sql", "full_refresh")),
        (DbtRunLocalOperator, ("env", "vars", "compiled_sql", "full_refresh")),
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


@patch("cosmos.operators.local.DbtLocalBaseOperator.store_compiled_sql")
@patch("cosmos.operators.local.DbtLocalBaseOperator.handle_exception_subprocess")
@patch("cosmos.config.ProfileConfig.ensure_profile")
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_subprocess")
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_dbt_runner")
@pytest.mark.parametrize("invocation_mode", [InvocationMode.SUBPROCESS, InvocationMode.DBT_RUNNER])
def test_operator_execute_deps_parameters(
    mock_dbt_runner,
    mock_subprocess,
    mock_ensure_profile,
    mock_exception_handling,
    mock_store_compiled_sql,
    invocation_mode,
):
    expected_call_kwargs = [
        "/usr/local/bin/dbt",
        "deps",
        "--profiles-dir",
        "/path/to",
        "--profile",
        "default",
        "--target",
        "dev",
    ]
    task = DbtRunLocalOperator(
        profile_config=real_profile_config,
        task_id="my-task",
        project_dir=DBT_PROJ_DIR,
        install_deps=True,
        emit_datasets=False,
        dbt_executable_path="/usr/local/bin/dbt",
        invocation_mode=invocation_mode,
    )
    mock_ensure_profile.return_value.__enter__.return_value = (Path("/path/to/profile"), {"ENV_VAR": "value"})
    task.execute(context={"task_instance": MagicMock()})
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
