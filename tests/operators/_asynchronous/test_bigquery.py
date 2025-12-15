from __future__ import annotations

import base64
import zlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
from cosmos.constants import PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS
from packaging import version
import pytest
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from tests.utils import test_dag as run_test_dag
from airflow import __version__ as airflow_version

from cosmos.config import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.operators._asynchronous.bigquery import (
    DbtRunAirflowAsyncBigqueryOperator,
    _configure_bigquery_async_op_args,
    _mock_bigquery_adapter,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

real_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


@pytest.fixture
def profile_config_mock():
    """Fixture to create a mock ProfileConfig."""
    mock_config = MagicMock(spec=ProfileConfig)
    mock_config.get_profile_type.return_value = "bigquery"
    mock_config.profile_mapping.conn_id = "google_cloud_default"
    mock_config.profile_mapping.profile = {"project": "test_project", "dataset": "test_dataset"}
    return mock_config


def test_dbt_run_airflow_async_bigquery_operator_init(profile_config_mock):
    """Test DbtRunAirflowAsyncBigqueryOperator initializes with correct attributes."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    assert isinstance(operator, DbtRunAirflowAsyncBigqueryOperator)
    assert isinstance(operator, BigQueryInsertJobOperator)
    assert operator.project_dir == "/path/to/project"
    assert operator.profile_config == profile_config_mock
    assert operator.gcp_conn_id == "google_cloud_default"
    assert operator.full_refresh is False  # Default value should be False


def test_dbt_run_airflow_async_bigquery_operator_base_cmd(profile_config_mock):
    """Test base_cmd property returns the correct dbt command."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    assert operator.base_cmd == ["run"]


def test_get_sql_from_xcom(profile_config_mock):
    fake_sql = "SELECT 42;"
    compressed_sql = zlib.compress(fake_sql.encode("utf-8"))
    compressed_b64_sql = base64.b64encode(compressed_sql).decode("utf-8")

    mock_context = {"ti": MagicMock()}

    mock_context["ti"].xcom_pull.return_value = compressed_b64_sql

    obj = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    obj.project_dir = "/tmp/project/subdir"

    obj.async_context = {
        "dbt_node_config": {"file_path": "/tmp/project/models_test.sql"},
        "dbt_dag_task_group_identifier": "tg1",
        "run_id": "rid123",
    }

    expected_key = "models_test.sql"

    result = obj.get_sql_from_xcom(mock_context)

    mock_context["ti"].xcom_pull.assert_called_once_with(
        task_ids="dbt_setup_async",
        key=expected_key,
    )
    assert result == fake_sql


@patch.object(DbtRunAirflowAsyncBigqueryOperator, "build_and_run_cmd")
@patch("cosmos.operators._asynchronous.bigquery.settings.enable_setup_async_task", False)
def test_dbt_run_airflow_async_bigquery_operator_execute(mock_build_and_run_cmd, profile_config_mock):
    """Test execute calls build_and_run_cmd with correct parameters."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.emit_datasets = False

    # Mock context with run_id
    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"  # For context["run_id"]

    operator.execute(mock_context)

    # Check that build_and_run_cmd was called with the correct parameters
    # but ignore the run_id in async_context for the assertion
    assert mock_build_and_run_cmd.call_count == 1
    args, kwargs = mock_build_and_run_cmd.call_args
    assert kwargs["context"] == mock_context
    assert kwargs["run_as_async"] is True
    assert kwargs["async_context"]["profile_type"] == "bigquery"
    assert kwargs["async_context"]["async_operator"] == BigQueryInsertJobOperator


@pytest.fixture
def async_operator_mock():
    """Fixture to create a mock async operator object."""
    return Mock()


@pytest.mark.integration
def test_mock_bigquery_adapter():
    """Test _mock_bigquery_adapter to verify it modifies BigQueryConnectionManager.execute."""
    from dbt.adapters.bigquery.connections import BigQueryConnectionManager

    _mock_bigquery_adapter()

    assert hasattr(BigQueryConnectionManager, "execute")

    response, table = BigQueryConnectionManager.execute(None, sql="SELECT 1")
    assert response._message == "mock_bigquery_adapter_response"
    assert table is not None


def test_configure_bigquery_async_op_args_valid(async_operator_mock):
    """Test _configure_bigquery_async_op_args correctly configures the async operator."""
    sql_query = "SELECT * FROM test_table"

    result = _configure_bigquery_async_op_args(async_operator_mock, sql=sql_query)

    assert result == async_operator_mock
    assert result.configuration["query"]["query"] == sql_query
    assert result.configuration["query"]["useLegacySql"] is False


def test_configure_bigquery_async_op_args_missing_sql(async_operator_mock):
    """Test _configure_bigquery_async_op_args raises CosmosValueError when 'sql' is missing."""
    with pytest.raises(CosmosValueError, match="Keyword argument 'sql' is required for BigQuery Async operator"):
        _configure_bigquery_async_op_args(async_operator_mock)


@patch("cosmos.settings.upload_sql_to_xcom", False)
@patch("cosmos.operators._asynchronous.bigquery.DbtRunAirflowAsyncBigqueryOperator.get_remote_sql")
@patch("cosmos.operators._asynchronous.bigquery.DbtRunAirflowAsyncBigqueryOperator._override_rtif")
def test_store_compiled_sql(mock_override_rtif, mock_get_remote_sql, profile_config_mock):
    from airflow.models.taskinstance import TaskInstance

    mock_get_remote_sql.return_value = "SELECT * FROM test_table;"

    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    mock_task_instance = Mock(spec=TaskInstance)
    mock_task_instance.task = operator
    mock_context = {"ti": mock_task_instance}

    operator._store_template_fields(mock_context)
    # check if gcp_project and dataset are set after the tasks gets executed

    assert operator.compiled_sql == "SELECT * FROM test_table;"
    assert operator.dataset == "test_dataset"
    assert operator.gcp_project == "test_project"
    mock_override_rtif.assert_called()


@patch("cosmos.operators._asynchronous.bigquery.DbtRunAirflowAsyncBigqueryOperator._store_template_fields")
def test_execute_complete(mock_store_sql, profile_config_mock):
    # Create a mock context with run_id
    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"  # For context["run_id"]

    mock_event = {"job_id": "test_job"}

    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.emit_datasets = False

    with patch.object(BigQueryInsertJobOperator, "execute_complete", return_value="test_job") as mock_super_execute:
        result = operator.execute_complete(mock_context, mock_event)

    assert result == "test_job"
    mock_super_execute.assert_called_once_with(context=mock_context, event=mock_event)
    mock_store_sql.assert_called_once_with(context=mock_context)


def test_dbt_run_airflow_async_bigquery_operator_with_full_refresh(profile_config_mock):
    """Test DbtRunAirflowAsyncBigqueryOperator initializes with full_refresh=True."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task", "full_refresh": True},
    )

    assert operator.full_refresh is True



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
        run_operator = DbtRunAirflowAsyncBigqueryOperator(
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
