from unittest.mock import MagicMock, patch

import pytest

from cosmos.config import ProfileConfig
from cosmos.operators._asynchronous.databricks import DbtRunAirflowAsyncDatabricksOperator


@pytest.fixture
def profile_config_mock():
    """Mock ProfileConfig for testing."""
    profile_mapping_mock = MagicMock()
    profile_mapping_mock.conn_id = "databricks_default"
    profile_mapping_mock.profile = {"catalog": "test_catalog", "schema": "test_schema", "http_path": "/sql/1.0/warehouses/test"}
    
    profile_config = MagicMock(spec=ProfileConfig)
    profile_config.profile_mapping = profile_mapping_mock
    profile_config.get_profile_type.return_value = "databricks"
    
    return profile_config


def test_dbt_run_airflow_async_databricks_operator_initialization(profile_config_mock):
    """Test that the operator initializes correctly."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    assert operator.task_id == "test_task"
    assert operator.project_dir == "/path/to/project"
    assert operator.databricks_conn_id == "databricks_default"
    assert operator.deferrable is True


def test_dbt_run_airflow_async_databricks_operator_base_cmd(profile_config_mock):
    """Test base_cmd property returns the correct dbt command."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    assert operator.base_cmd == ["run"]


@patch("cosmos.operators._asynchronous.databricks.DatabricksSqlOperator.execute")
@patch.object(DbtRunAirflowAsyncDatabricksOperator, "get_sql_from_xcom")
@patch("cosmos.operators._asynchronous.databricks.settings.enable_setup_async_task", True)
@patch("cosmos.operators._asynchronous.databricks.settings.upload_sql_to_xcom", True)
def test_dbt_run_airflow_async_databricks_operator_execute_with_xcom(
    mock_get_sql, mock_super_execute, profile_config_mock
):
    """Test execute calls get_sql_from_xcom and parent execute when using XCom."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    
    operator.emit_datasets = False
    mock_get_sql.return_value = "SELECT * FROM test_table"
    
    # Mock context with run_id
    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"
    
    operator.execute(mock_context)
    
    # Verify SQL was fetched from XCom
    mock_get_sql.assert_called_once()
    # Verify parent execute was called
    mock_super_execute.assert_called_once()
    # Verify SQL was set
    assert operator.sql == "SELECT * FROM test_table"


@patch("cosmos.operators._asynchronous.databricks.DatabricksSqlOperator.execute")
@patch.object(DbtRunAirflowAsyncDatabricksOperator, "get_remote_sql")
@patch("cosmos.operators._asynchronous.databricks.settings.enable_setup_async_task", True)
@patch("cosmos.operators._asynchronous.databricks.settings.upload_sql_to_xcom", False)
def test_dbt_run_airflow_async_databricks_operator_execute_with_remote(
    mock_get_remote_sql, mock_super_execute, profile_config_mock
):
    """Test execute calls get_remote_sql when not using XCom."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    
    operator.emit_datasets = False
    mock_get_remote_sql.return_value = "SELECT * FROM remote_table"
    
    # Mock context with run_id
    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"
    
    operator.execute(mock_context)
    
    # Verify SQL was fetched from remote
    mock_get_remote_sql.assert_called_once()
    # Verify parent execute was called
    mock_super_execute.assert_called_once()
    # Verify SQL was set
    assert operator.sql == "SELECT * FROM remote_table"


@patch.object(DbtRunAirflowAsyncDatabricksOperator, "build_and_run_cmd")
@patch("cosmos.operators._asynchronous.databricks.settings.enable_setup_async_task", False)
def test_dbt_run_airflow_async_databricks_operator_execute_without_setup(
    mock_build_and_run_cmd, profile_config_mock
):
    """Test execute calls build_and_run_cmd when setup task is disabled."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    
    operator.emit_datasets = False
    
    # Mock context with run_id
    mock_context = MagicMock()
    mock_context.__getitem__.return_value = "test_run_id"
    
    operator.execute(mock_context)
    
    # Check that build_and_run_cmd was called with the correct parameters
    assert mock_build_and_run_cmd.call_count == 1
    args, kwargs = mock_build_and_run_cmd.call_args
    assert kwargs["run_as_async"] is True
    assert "async_context" in kwargs


def test_register_event_with_catalog(profile_config_mock):
    """Test that dataset events are registered correctly with catalog."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    
    operator.catalog = "test_catalog"
    operator.schema = "test_schema"
    operator.async_context = {
        "dbt_node_config": {
            "unique_id": "model.my_project.my_model"
        }
    }
    
    mock_context = MagicMock()
    
    with patch.object(operator, "register_dataset") as mock_register:
        operator._register_event(mock_context)
        
        # Check that register_dataset was called
        mock_register.assert_called_once()
        args = mock_register.call_args[0]
        
        # Verify the asset URI format
        outputs = args[1]
        assert len(outputs) == 1
        assert outputs[0].uri == "databricks://test_catalog.test_schema.my_model"


def test_register_event_without_catalog(profile_config_mock):
    """Test that dataset events are registered correctly without catalog."""
    operator = DbtRunAirflowAsyncDatabricksOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    
    operator.catalog = ""
    operator.schema = "test_schema"
    operator.async_context = {
        "dbt_node_config": {
            "unique_id": "model.my_project.my_model"
        }
    }
    
    mock_context = MagicMock()
    
    with patch.object(operator, "register_dataset") as mock_register:
        operator._register_event(mock_context)
        
        # Check that register_dataset was called
        mock_register.assert_called_once()
        args = mock_register.call_args[0]
        
        # Verify the asset URI format
        outputs = args[1]
        assert len(outputs) == 1
        assert outputs[0].uri == "databricks://test_schema.my_model"
