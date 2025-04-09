# TODO: Implement it
from __future__ import annotations

from typing import Any

try:  # Airflow 3
    from airflow.sdk.bases.operator import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator
from airflow.utils.context import Context


class DbtRunAirflowAsyncDatabricksOperator(BaseOperator):  # type: ignore[misc]
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        raise NotImplementedError()
