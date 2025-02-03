from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from cosmos.config import ProfileConfig
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator


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
        full_refresh=True,
    )

    assert isinstance(operator, DbtRunAirflowAsyncBigqueryOperator)
    assert isinstance(operator, BigQueryInsertJobOperator)
    assert operator.project_dir == "/path/to/project"
    assert operator.profile_config == profile_config_mock
    assert operator.gcp_conn_id == "google_cloud_default"
    assert operator.gcp_project == "test_project"
    assert operator.dataset == "test_dataset"


def test_dbt_run_airflow_async_bigquery_operator_base_cmd(profile_config_mock):
    """Test base_cmd property returns the correct dbt command."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )
    assert operator.base_cmd == ["run"]


@patch.object(DbtRunAirflowAsyncBigqueryOperator, "build_and_run_cmd")
def test_dbt_run_airflow_async_bigquery_operator_execute(mock_build_and_run_cmd, profile_config_mock):
    """Test execute calls build_and_run_cmd with correct parameters."""
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    mock_context = MagicMock()
    operator.execute(mock_context)

    mock_build_and_run_cmd.assert_called_once_with(
        context=mock_context,
        run_as_async=True,
        async_context={
            "profile_type": "bigquery",
            "async_operator": BigQueryInsertJobOperator,
        },
    )
