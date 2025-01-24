from unittest.mock import patch

import pytest

from cosmos import ProfileConfig
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator, _create_async_operator_class
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.profiles import get_automatic_profile_mapping


@pytest.mark.parametrize(
    "profile_type, dbt_class, expected_operator_class",
    [
        ("bigquery", "DbtRun", DbtRunAirflowAsyncBigqueryOperator),
        ("snowflake", "DbtRun", DbtRunLocalOperator),
        ("bigquery", "DbtTest", DbtRunLocalOperator),
    ],
)
def test_create_async_operator_class_success(profile_type, dbt_class, expected_operator_class):
    """Test the successful loading of the async operator class."""

    operator_class = _create_async_operator_class(profile_type, dbt_class)

    assert operator_class == expected_operator_class


@patch("cosmos.operators._asynchronous.bigquery.DbtRunAirflowAsyncBigqueryOperator.drop_table_sql")
@patch("cosmos.operators._asynchronous.bigquery.DbtRunAirflowAsyncBigqueryOperator.get_remote_sql")
@patch("cosmos.operators._asynchronous.bigquery.BigQueryInsertJobOperator.execute")
def test_factory_async_class(mock_execute, get_remote_sql, drop_table_sql, mock_bigquery_conn):
    profile_mapping = get_automatic_profile_mapping(
        mock_bigquery_conn.conn_id,
        profile_args={
            "dataset": "my_dataset",
        },
    )
    bigquery_profile_config = ProfileConfig(
        profile_name="my_profile", target_name="dev", profile_mapping=profile_mapping
    )
    factory_class = DbtRunAirflowAsyncFactoryOperator(
        task_id="run",
        project_dir="/tmp",
        profile_config=bigquery_profile_config,
        full_refresh=True,
        extra_context={"dbt_node_config": {"resource_name": "customer"}},
    )

    async_operator = factory_class.create_async_operator()
    assert async_operator == DbtRunAirflowAsyncBigqueryOperator

    factory_class.execute(context={})

    mock_execute.assert_called_once_with({})
