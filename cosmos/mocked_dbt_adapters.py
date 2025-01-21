from cosmos.constants import BIGQUERY_PROFILE_TYPE


def mock_bigquery_adapter() -> None:
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
    BIGQUERY_PROFILE_TYPE: mock_bigquery_adapter,
}
