from datetime import datetime
from pathlib import Path

import pytest

from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig
from cosmos.operators.airflow_async import (
    DbtBuildAirflowAsyncOperator,
    DbtCompileAirflowAsyncOperator,
    DbtLSAirflowAsyncOperator,
    DbtRunOperationAirflowAsyncOperator,
    DbtSeedAirflowAsyncOperator,
    DbtSnapshotAirflowAsyncOperator,
    DbtSourceAirflowAsyncOperator,
    DbtTestAirflowAsyncOperator,
)
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCompileLocalOperator,
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)
from cosmos.profiles import get_automatic_profile_mapping

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "original_jaffle_shop"


@pytest.mark.integration
def test_airflow_async_operator_init(mock_bigquery_conn):
    """Test that Airflow can correctly parse an async operator with operator args"""
    profile_mapping = get_automatic_profile_mapping(mock_bigquery_conn.conn_id, {})

    profile_config = ProfileConfig(
        profile_name="airflow_db",
        target_name="bq",
        profile_mapping=profile_mapping,
    )

    DbtDag(
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR / DBT_PROJECT_NAME),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.AIRFLOW_ASYNC,
        ),
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="simple_dag_async",
        operator_args={"location": "us", "install_deps": True},
    )


def test_dbt_build_airflow_async_operator_inheritance():
    assert issubclass(DbtBuildAirflowAsyncOperator, DbtBuildLocalOperator)


def test_dbt_ls_airflow_async_operator_inheritance():
    assert issubclass(DbtLSAirflowAsyncOperator, DbtLSLocalOperator)


def test_dbt_seed_airflow_async_operator_inheritance():
    assert issubclass(DbtSeedAirflowAsyncOperator, DbtSeedLocalOperator)


def test_dbt_snapshot_airflow_async_operator_inheritance():
    assert issubclass(DbtSnapshotAirflowAsyncOperator, DbtSnapshotLocalOperator)


def test_dbt_source_airflow_async_operator_inheritance():
    assert issubclass(DbtSourceAirflowAsyncOperator, DbtSourceLocalOperator)


def test_dbt_test_airflow_async_operator_inheritance():
    assert issubclass(DbtTestAirflowAsyncOperator, DbtTestLocalOperator)


def test_dbt_run_operation_airflow_async_operator_inheritance():
    assert issubclass(DbtRunOperationAirflowAsyncOperator, DbtRunOperationLocalOperator)


def test_dbt_compile_airflow_async_operator_inheritance():
    assert issubclass(DbtCompileAirflowAsyncOperator, DbtCompileLocalOperator)
