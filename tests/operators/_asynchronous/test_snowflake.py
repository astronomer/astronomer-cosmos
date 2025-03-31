from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

from cosmos.config import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.operators._asynchronous.snowflake import (
    DbtRunAirflowAsyncSnowflakeOperator,
    _configure_snowflake_async_op_args,
)


@pytest.fixture
def profile_config_mock():
    mock_config = MagicMock(spec=ProfileConfig)
    mock_config.get_profile_type.return_value = "snowflake"
    mock_config.profile_mapping.conn_id = "snowflake_default"
    mock_config.profile_mapping.profile = {"dataset": "test_dataset"}
    return mock_config


@pytest.fixture
def async_operator_mock():
    """Fixture to create a mock async operator object."""
    return Mock()


def test_dbt_run_airflow_async_snowflake_operator_init(profile_config_mock):
    operator = DbtRunAirflowAsyncSnowflakeOperator(
        task_id="test_task",
        project_dir="/path/to/project",
        profile_config=profile_config_mock,
        dbt_kwargs={"task_id": "test_task"},
    )

    assert isinstance(operator, DbtRunAirflowAsyncSnowflakeOperator)
    assert isinstance(operator, SnowflakeSqlApiOperator)
    assert operator.project_dir == "/path/to/project"
    assert operator.profile_config == profile_config_mock
    assert operator.snowflake_conn_id == "snowflake_default"

    # This should start as None
    assert operator.sql is None


def test_configure_bigquery_async_op_args_missing_sql(async_operator_mock):
    """Test _configure_bigquery_async_op_args raises CosmosValueError when 'sql' is missing."""
    with pytest.raises(CosmosValueError, match="Keyword argument 'sql' is required for Snowflake Async operator"):
        _configure_snowflake_async_op_args(async_operator_mock)
