import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtDocsAzureStorageLocalOperator,
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


def test_dbt_base_operator_add_global_flags() -> None:
    dbt_base_operator = DbtLocalBaseOperator(
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
    dbt_base_operator = DbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        base_cmd=["run"],
        dbt_cmd_flags=["--full-refresh"],
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    assert cmd[-2] == "run"
    assert cmd[-1] == "--full-refresh"


def test_dbt_base_operator_add_user_supplied_global_flags() -> None:
    dbt_base_operator = DbtLocalBaseOperator(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        base_cmd=["run"],
        dbt_cmd_global_flags=["--cache-selected-only"],
    )

    cmd, _ = dbt_base_operator.build_cmd(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    assert cmd[-2] == "--cache-selected-only"
    assert cmd[-1] == "run"


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
    dbt_base_operator = DbtLocalBaseOperator(
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
    dbt_base_operator = DbtLocalBaseOperator(
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
            profile_config=mini_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
        )
        test_operator = DbtTestLocalOperator(
            profile_config=mini_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dbt_cmd_flags=["--models", "stg_customers"],
            install_deps=True,
        )
        run_operator
    run_test_dag(dag)
    assert run_operator.inlets == []
    assert run_operator.outlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.inlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.outlets == []


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

    dbt_base_operator = DbtLocalBaseOperator(
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
    dbt_base_operator = DbtLocalBaseOperator(
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
    dbt_base_operator = DbtLocalBaseOperator(
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

    dbt_base_operator = DbtLocalBaseOperator(
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
        DbtDocsLocalOperator,
        DbtDocsS3LocalOperator,
        DbtDocsAzureStorageLocalOperator,
    ),
)
@patch("cosmos.operators.local.DbtLocalBaseOperator.build_and_run_cmd")
def test_operator_execute_without_flags(mock_build_and_run_cmd, operator_class):
    operator_class_kwargs = {
        DbtDocsS3LocalOperator: {"aws_conn_id": "fake-conn", "bucket_name": "fake-bucket"},
        DbtDocsAzureStorageLocalOperator: {"azure_conn_id": "fake-conn", "container_name": "fake-container"},
    }
    task = operator_class(
        profile_config=profile_config,
        task_id="my-task",
        project_dir="my/dir",
        **operator_class_kwargs.get(operator_class, {}),
    )
    task.execute(context={})
    mock_build_and_run_cmd.assert_called_once_with(context={})
