import pytest
from airflow import __version__ as airflow_version
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from packaging import version

from cosmos.operators.airflow_async import (
    DbtBuildAirflowAsyncOperator,
    DbtCompileAirflowAsyncOperator,
    DbtLSAirflowAsyncOperator,
    DbtRunAirflowAsyncOperator,
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


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.8"),
    reason="Cosmos Async operators only work with Airflow 2.8 onwards.",
)
def test_dbt_run_airflow_async_operator_inheritance():
    assert issubclass(DbtRunAirflowAsyncOperator, BigQueryInsertJobOperator)


def test_dbt_test_airflow_async_operator_inheritance():
    assert issubclass(DbtTestAirflowAsyncOperator, DbtTestLocalOperator)


def test_dbt_run_operation_airflow_async_operator_inheritance():
    assert issubclass(DbtRunOperationAirflowAsyncOperator, DbtRunOperationLocalOperator)


def test_dbt_compile_airflow_async_operator_inheritance():
    assert issubclass(DbtCompileAirflowAsyncOperator, DbtCompileLocalOperator)
