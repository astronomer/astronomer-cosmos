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
