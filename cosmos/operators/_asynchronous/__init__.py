from __future__ import annotations

from typing import Any

from airflow.utils.context import Context

from cosmos.operators.local import DbtRunLocalOperator as DbtRunOperator


class SetupAsyncOperator(DbtRunOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs["emit_datasets"] = False
        super().__init__(*args, **kwargs)

    def execute(self, context: Context, **kwargs: Any) -> None:
        async_context = {"profile_type": self.profile_config.get_profile_type()}
        self.build_and_run_cmd(
            context=context, cmd_flags=self.dbt_cmd_flags, run_as_async=True, async_context=async_context
        )


class TeardownAsyncOperator(DbtRunOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs["emit_datasets"] = False
        super().__init__(*args, **kwargs)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        async_context = {"profile_type": self.profile_config.get_profile_type(), "teardown_task": True}
        self.build_and_run_cmd(
            context=context, cmd_flags=self.dbt_cmd_flags, run_as_async=True, async_context=async_context
        )
