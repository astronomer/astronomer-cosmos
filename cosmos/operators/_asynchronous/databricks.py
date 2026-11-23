# TODO: Implement it
from __future__ import annotations

from typing import Any

try:
    from airflow.sdk.definitions.context import Context
except ImportError:
    from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except (ImportError, AttributeError):
    from airflow.models import BaseOperator  # Airflow 2


# Airflow 2 resolves the compatibility base as Any; Airflow 3 exposes its type.
class DbtRunAirflowAsyncDatabricksOperator(BaseOperator):  # type: ignore[misc, unused-ignore]
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        raise NotImplementedError()
