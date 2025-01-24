from unittest.mock import MagicMock, patch

import pytest

from cosmos import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.profiles import get_automatic_profile_mapping
from cosmos.settings import AIRFLOW_IO_AVAILABLE


def test_get_remote_sql_airflow_io_unavailable(mock_bigquery_conn):
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
        },
    )
    bigquery_profile_config = ProfileConfig(
        profile_name="my_profile", target_name="dev", profile_mapping=profile_mapping
    )
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task", project_dir="/tmp", profile_config=bigquery_profile_config
    )

    operator.extra_context = {
        "dbt_node_config": {"file_path": "/some/path/to/file.sql"},
        "dbt_dag_task_group_identifier": "task_group_1",
    }

    if not AIRFLOW_IO_AVAILABLE:
        with pytest.raises(
            CosmosValueError, match="Cosmos async support is only available starting in Airflow 2.8 or later."
        ):
            operator.get_remote_sql()


def test_get_remote_sql_success(mock_bigquery_conn):
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
        },
    )
    bigquery_profile_config = ProfileConfig(
        profile_name="my_profile", target_name="dev", profile_mapping=profile_mapping
    )
    operator = DbtRunAirflowAsyncBigqueryOperator(
        task_id="test_task", project_dir="/tmp", profile_config=bigquery_profile_config
    )

    operator.extra_context = {
        "dbt_node_config": {"file_path": "/some/path/to/file.sql"},
        "dbt_dag_task_group_identifier": "task_group_1",
    }
    operator.project_dir = "/tmp"

    mock_object_storage_path = MagicMock()
    mock_file = MagicMock()
    mock_file.read.return_value = "SELECT * FROM table"

    mock_object_storage_path.open.return_value.__enter__.return_value = mock_file

    with patch("airflow.io.path.ObjectStoragePath", return_value=mock_object_storage_path):
        remote_sql = operator.get_remote_sql()

    assert remote_sql == "SELECT * FROM table"
    mock_object_storage_path.open.assert_called_once()
