import logging
import os
import sys
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from airflow import DAG
from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessResult
from airflow.utils.context import Context
from packaging import version
from pendulum import datetime

from cosmos.config import ProfileConfig
from cosmos.operators.local import (
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtSnapshotLocalOperator,
    DbtRunLocalOperator,
    DbtTestLocalOperator,
    DbtBuildLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtDocsAzureStorageLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtSeedLocalOperator,
    DbtRunOperationLocalOperator,
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
    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
    )
    if expected_exception:
        with pytest.raises(expected_exception):
            dbt_base_operator.exception_handling(SubprocessResult(exception_code_returned, None))
    else:
        dbt_base_operator.exception_handling(SubprocessResult(exception_code_returned, None))


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


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.4"),
    reason="Airflow DAG did not have datasets until the 2.4 release",
)
@pytest.mark.integration
def test_run_operator_dataset_inlets_and_outlets():
    from airflow.datasets import Dataset

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
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
        run_operator
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
def test_run_test_operator_with_callback(failing_test_dbt_project):
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
        )
        run_operator >> test_operator
    run_test_dag(dag)
    assert on_warning_callback.called


@pytest.mark.integration
def test_run_test_operator_without_callback():
    on_warning_callback = MagicMock()

    with DAG("test-id-3", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtSeedLocalOperator(
            profile_config=mini_profile_config,
            project_dir=MINI_DBT_PROJ_DIR,
            task_id="run",
            append_env=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=mini_profile_config,
            project_dir=MINI_DBT_PROJ_DIR,
            task_id="test",
            append_env=True,
            on_warning_callback=on_warning_callback,
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
        (DbtSeedLocalOperator, {"full_refresh": True}, {"context": {}, "cmd_flags": ["--full-refresh"]}),
        (DbtRunLocalOperator, {"full_refresh": True}, {"context": {}, "cmd_flags": ["--full-refresh"]}),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "select": ["tag:daily"], "exclude": ["tag:disabled"]},
            {"context": {}, "cmd_flags": ["--exclude", "tag:disabled", "--select", "tag:daily"]},
        ),
        (
            DbtTestLocalOperator,
            {"full_refresh": True, "selector": "nightly_snowplow"},
            {"context": {}, "cmd_flags": ["--selector", "nightly_snowplow"]},
        ),
        (
            DbtRunOperationLocalOperator,
            {"args": {"days": 7, "dry_run": True}, "macro_name": "bla"},
            {"context": {}, "cmd_flags": ["--args", "days: 7\ndry_run: true\n"]},
        ),
    ],
)
@patch("cosmos.operators.local.DbtLocalBaseOperator.build_and_run_cmd")
def test_operator_execute_with_flags(mock_build_and_run_cmd, operator_class, kwargs, expected_call_kwargs):
    task = operator_class(profile_config=profile_config, task_id="my-task", project_dir="my/dir", **kwargs)
    task.execute(context={})
    mock_build_and_run_cmd.assert_called_once_with(**expected_call_kwargs)


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
@patch("cosmos.operators.local.DbtLocalBaseOperator.exception_handling")
@patch("cosmos.config.ProfileConfig.ensure_profile")
@patch("cosmos.operators.local.DbtLocalBaseOperator.run_subprocess")
def test_operator_execute_deps_parameters(
    mock_build_and_run_cmd, mock_ensure_profile, mock_exception_handling, mock_store_compiled_sql
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
    )
    mock_ensure_profile.return_value.__enter__.return_value = (Path("/path/to/profile"), {"ENV_VAR": "value"})
    task.execute(context={"task_instance": MagicMock()})
    assert mock_build_and_run_cmd.call_args_list[0].kwargs["command"] == expected_call_kwargs


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
        profile_config=profile_config, task_id="my-task", project_dir="my/dir", cancel_query_on_kill=True
    )

    dbt_base_operator.on_kill()

    mock_send_sigint.assert_called_once()


@patch("cosmos.hooks.subprocess.FullOutputSubprocessHook.send_sigterm")
def test_dbt_local_operator_on_kill_sigterm(mock_send_sigterm) -> None:

    dbt_base_operator = ConcreteDbtLocalBaseOperator(
        profile_config=profile_config, task_id="my-task", project_dir="my/dir", cancel_query_on_kill=False
    )

    dbt_base_operator.on_kill()

    mock_send_sigterm.assert_called_once()
