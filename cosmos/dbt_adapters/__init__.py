from __future__ import annotations

from typing import Any

from cosmos.constants import BIGQUERY_PROFILE_TYPE
from cosmos.dbt_adapters.bigquery import _associate_bigquery_async_op_args, _mock_bigquery_adapter

PROFILE_TYPE_MOCK_ADAPTER_CALLABLE_MAP = {
    BIGQUERY_PROFILE_TYPE: _mock_bigquery_adapter,
}

PROFILE_TYPE_ASSOCIATE_ARGS_CALLABLE_MAP = {
    BIGQUERY_PROFILE_TYPE: _associate_bigquery_async_op_args,
}


def associate_async_operator_args(async_operator_obj: Any, profile_type: str, **kwargs: Any) -> Any:
    return PROFILE_TYPE_ASSOCIATE_ARGS_CALLABLE_MAP[profile_type](async_operator_obj, **kwargs)
