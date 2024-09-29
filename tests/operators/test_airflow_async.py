from cosmos.operators.airflow_async import (
    DbtBuildAirflowAsyncOperator,
    DbtCompileAirflowAsyncOperator,
    DbtDocsAirflowAsyncOperator,
    DbtDocsAzureStorageAirflowAsyncOperator,
    DbtDocsGCSAirflowAsyncOperator,
    DbtDocsS3AirflowAsyncOperator,
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
    DbtDocsAzureStorageLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
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


def test_dbt_run_airflow_async_operator_inheritance():
    assert issubclass(DbtRunAirflowAsyncOperator, DbtRunLocalOperator)


def test_dbt_test_airflow_async_operator_inheritance():
    assert issubclass(DbtTestAirflowAsyncOperator, DbtTestLocalOperator)


def test_dbt_run_operation_airflow_async_operator_inheritance():
    assert issubclass(DbtRunOperationAirflowAsyncOperator, DbtRunOperationLocalOperator)


def test_dbt_docs_airflow_async_operator_inheritance():
    assert issubclass(DbtDocsAirflowAsyncOperator, DbtDocsLocalOperator)


def test_dbt_docs_s3_airflow_async_operator_inheritance():
    assert issubclass(DbtDocsS3AirflowAsyncOperator, DbtDocsS3LocalOperator)


def test_dbt_docs_azure_storage_airflow_async_operator_inheritance():
    assert issubclass(DbtDocsAzureStorageAirflowAsyncOperator, DbtDocsAzureStorageLocalOperator)


def test_dbt_docs_gcs_airflow_async_operator_inheritance():
    assert issubclass(DbtDocsGCSAirflowAsyncOperator, DbtDocsGCSLocalOperator)


def test_dbt_compile_airflow_async_operator_inheritance():
    assert issubclass(DbtCompileAirflowAsyncOperator, DbtCompileLocalOperator)
