from __future__ import annotations

from typing import Any

from cosmos.constants import BIGQUERY_PROFILE_TYPE
from cosmos.exceptions import CosmosValueError


def _mock_bigquery_adapter() -> None:
    from typing import Optional, Tuple

    import agate
    from dbt.adapters.bigquery.connections import BigQueryAdapterResponse, BigQueryConnectionManager
    from dbt_common.clients.agate_helper import empty_table

    def execute(  # type: ignore[no-untyped-def]
        self, sql, auto_begin=False, fetch=None, limit: Optional[int] = None
    ) -> Tuple[BigQueryAdapterResponse, agate.Table]:
        return BigQueryAdapterResponse("mock_bigquery_adapter_response"), empty_table()

    BigQueryConnectionManager.execute = execute


PROFILE_TYPE_MOCK_ADAPTER_CALLABLE_MAP = {
    BIGQUERY_PROFILE_TYPE: _mock_bigquery_adapter,
}


def _associate_bigquery_async_op_args(async_op_obj: Any, **kwargs: Any) -> Any:
    sql = kwargs.get("sql")
    if not sql:
        raise CosmosValueError("Keyword argument 'sql' is required for BigQuery Async operator")
    async_op_obj.configuration = {
        "query": {
            "query": sql,
            "useLegacySql": False,
        }
    }
    return async_op_obj


PROFILE_TYPE_ASSOCIATE_ARGS_CALLABLE_MAP = {
    BIGQUERY_PROFILE_TYPE: _associate_bigquery_async_op_args,
}


def _associate_async_operator_args(async_operator_obj: Any, profile_type: str, **kwargs: Any) -> Any:
    return PROFILE_TYPE_ASSOCIATE_ARGS_CALLABLE_MAP[profile_type](async_operator_obj, **kwargs)
