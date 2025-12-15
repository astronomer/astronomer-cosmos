from __future__ import annotations

import base64
import zlib
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from packaging.version import Version

from cosmos.config import ProfileConfig
from cosmos.constants import AIRFLOW_VERSION
from cosmos.exceptions import CosmosValueError
from cosmos.operators._asynchronous.bigquery import (
    DbtRunAirflowAsyncBigqueryOperator,
    _configure_bigquery_async_op_args,
    _mock_bigquery_adapter,
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


@patch.object(DbtRunAirflowAsyncBigqueryOperator, "build_and_run_cmd")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_store_template_fields")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_register_event")
@patch("cosmos.operators._asynchronous.bigquery.settings.enable_setup_async_task", False)
def test_execute_calls_register_event_when_emit_datasets_true(
    mock_register_event, mock_store_template_fields, mock_build_and_run_cmd, profile_config_mock
):
    """Test that _register_event is called when emit_datasets=True in execute method."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.emit_datasets = True
    operator.gcp_project = "test_project"
    operator.dataset = "test_dataset"

    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"

    operator.execute(mock_context)

    mock_register_event.assert_called_once_with(mock_context)


@patch.object(DbtRunAirflowAsyncBigqueryOperator, "build_and_run_cmd")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_store_template_fields")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_register_event")
@patch("cosmos.operators._asynchronous.bigquery.settings.enable_setup_async_task", False)
def test_execute_does_not_call_register_event_when_emit_datasets_false(
    mock_register_event, mock_store_template_fields, mock_build_and_run_cmd, profile_config_mock
):
    """Test that _register_event is NOT called when emit_datasets=False in execute method."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.emit_datasets = False

    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"

    operator.execute(mock_context)

    mock_register_event.assert_not_called()


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.10.0"), reason="Require Airflow >= 2.10")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_store_template_fields")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "_register_event")
def test_execute_complete_calls_register_event_when_emit_datasets_true(
    mock_register_event, mock_store_template_fields, profile_config_mock
):
    """Test that _register_event is called when emit_datasets=True in execute_complete method."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.emit_datasets = True
    operator.gcp_project = "test_project"
    operator.dataset = "test_dataset"

    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"
    mock_event = {"job_id": "test_job"}

    with patch.object(BigQueryInsertJobOperator, "execute_complete", return_value="test_job"):
        operator.execute_complete(mock_context, mock_event)

    mock_register_event.assert_called_once_with(mock_context)


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.10.0"), reason="Require Airflow >= 2.10")
@patch.object(DbtRunAirflowAsyncBigqueryOperator, "register_dataset")
def test_register_event_with_uri(mock_register_dataset, profile_config_mock):
    """Test that _register_event correctly extracts table name from complex unique_id."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    operator.gcp_project = "my_project"
    operator.dataset = "my_dataset"
    operator.async_context = {"dbt_node_config": {"unique_id": "model.my_project.my_schema.my_complex_table_name"}}

    mock_context = MagicMock()

    operator._register_event(mock_context)

    # Verify register_dataset was called with correct table name (last part of unique_id)
    mock_register_dataset.assert_called_once()
    args, kwargs = mock_register_dataset.call_args
    assert args[0] == []  # inlets
    assert len(args[1]) == 1  # outlets
    if AIRFLOW_VERSION >= Version("3.0.0"):
        assert args[1][0].uri == "bigquery://my_project/my_dataset/my_complex_table_name"
    else:
        assert args[1][0].uri == "bigquery://my_project.my_dataset.my_complex_table_name/"
