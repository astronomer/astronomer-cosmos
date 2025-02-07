from __future__ import annotations

from typing import Any

from airflow.utils.context import Context

from cosmos.operators.local import DbtRunLocalOperator as DbtRunOperator
from cosmos.settings import enable_setup_task


class SetupAsyncOperator(DbtRunOperator):
    def execute(self, context: Context, **kwargs: Any) -> None:
        # TODO: Fix hardcoded values
        async_context = {"enable_setup_task": enable_setup_task, "profile_type": "bigquery"}
        self.build_and_run_cmd(context=context, run_as_async=True, async_context=async_context)
