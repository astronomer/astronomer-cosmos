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
from cosmos.operators.local import DbtLocalBaseOperator, DbtRunLocalOperator, DbtTestLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from tests.utils import test_dag as run_test_dag


DBT_PROJ_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"

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
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="run",
            dbt_cmd_flags=["--models", "stg_customers"],
        )
        test_operator = DbtTestLocalOperator(
            profile_config=real_profile_config,
            project_dir=DBT_PROJ_DIR,
            task_id="test",
            dbt_cmd_flags=["--models", "stg_customers"],
        )
        run_operator
    run_test_dag(dag)
    assert run_operator.inlets == []
    assert run_operator.outlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.inlets == [Dataset(uri="postgres://0.0.0.0:5432/postgres.public.stg_customers", extra=None)]
    assert test_operator.outlets == []


def test_run_operator_emits_events():
    class MockRun:
        facets = [3]

    class MockJob:
        facets = [4]

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
    facets = dbt_base_operator.get_openlineage_facets_on_complete()
    assert facets.inputs == [1]
    assert facets.outputs == [2]
    assert facets.run_facets == [3]
    assert facets.job_facets == [4]


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
