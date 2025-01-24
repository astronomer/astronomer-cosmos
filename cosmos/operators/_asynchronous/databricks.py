# pragma: no cover
# TODO: Implement it

from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class DbtRunAirflowAsyncDatabricksOperator(BaseOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        raise NotImplementedError()
