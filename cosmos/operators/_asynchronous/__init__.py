from __future__ import annotations

import inspect
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosmos.operators.local import DbtRunLocalOperator

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos._utils.importer import load_method_from_module
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.operators.virtualenv import DbtRunVirtualenvOperator


class SetupAsyncOperator(DbtRunVirtualenvOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs["emit_datasets"] = False
        super().__init__(*args, **kwargs)

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str) -> FullOutputSubprocessResult:
        # SAFELY inject mock at runtime (in memory)
        profile_type = self.profile_config.get_profile_type()
        mock_module = f"cosmos.operators._asynchronous.{profile_type}"
        mock_func = f"_mock_{profile_type}_adapter"
        mock = load_method_from_module(mock_module, mock_func)
        mock()  # Safe in-memory patch

        return super().run_subprocess(command, env, cwd)

    def execute(self, context: Context, **kwargs: Any) -> None:
        async_context = {"profile_type": self.profile_config.get_profile_type(), "run_id": context["run_id"]}
        self.build_and_run_cmd(
            context=context, cmd_flags=self.dbt_cmd_flags, run_as_async=True, async_context=async_context
        )


class TeardownAsyncOperator(DbtRunLocalOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs["emit_datasets"] = False
        super().__init__(*args, **kwargs)

    def execute(self, context: Context, **kwargs: Any) -> Any:

        dest_target_dir, dest_conn_id = self._configure_remote_target_path()

        from airflow.io.path import ObjectStoragePath

        dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]
        run_id = context["run_id"]
        run_dir_path_str = f"{str(dest_target_dir).rstrip('/')}/{dag_task_group_identifier}/{run_id}"

        run_dir_path = ObjectStoragePath(run_dir_path_str, conn_id=dest_conn_id)

        if run_dir_path.exists():
            run_dir_path.rmdir(recursive=True)
