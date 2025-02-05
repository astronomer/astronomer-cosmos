from __future__ import annotations

from unittest.mock import Mock

import pytest

from cosmos.dbt_adapters.bigquery import _associate_bigquery_async_op_args, _mock_bigquery_adapter
from cosmos.exceptions import CosmosValueError


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


def test_associate_bigquery_async_op_args_valid(async_operator_mock):
    """Test _associate_bigquery_async_op_args correctly configures the async operator."""
    sql_query = "SELECT * FROM test_table"

    result = _associate_bigquery_async_op_args(async_operator_mock, sql=sql_query)

    assert result == async_operator_mock
    assert result.configuration["query"]["query"] == sql_query
    assert result.configuration["query"]["useLegacySql"] is False


def test_associate_bigquery_async_op_args_missing_sql(async_operator_mock):
    """Test _associate_bigquery_async_op_args raises CosmosValueError when 'sql' is missing."""
    with pytest.raises(CosmosValueError, match="Keyword argument 'sql' is required for BigQuery Async operator"):
        _associate_bigquery_async_op_args(async_operator_mock)
