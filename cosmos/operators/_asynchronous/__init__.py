from __future__ import annotations

import inspect
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from airflow.sdk import ObjectStoragePath
except ImportError:
    try:
        from airflow.io.path import ObjectStoragePath
    except ImportError:
        pass

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
        profile_type = self.profile_config.get_profile_type()
        if not self._py_bin:
            raise AttributeError("_py_bin attribute not set for VirtualEnv operator")
        dbt_executable_path = str(Path(self._py_bin).parent / "dbt")
        asynchronous_operator_module = f"cosmos.operators._asynchronous.{profile_type}"
        mock_function_name = f"_mock_{profile_type}_adapter"
        mock_function = load_method_from_module(asynchronous_operator_module, mock_function_name)
        mock_function_full_source = inspect.getsource(mock_function)
        mock_function_body = textwrap.dedent("\n".join(mock_function_full_source.split("\n")[1:]))

        with open(dbt_executable_path) as f:
            dbt_entrypoint_script = f.readlines()
        if dbt_entrypoint_script[0].startswith("#!"):
            dbt_entrypoint_script.insert(1, mock_function_body)
        with open(dbt_executable_path, "w") as f:
            f.writelines(dbt_entrypoint_script)

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

        dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]
        run_id = context["run_id"]
        run_dir_path_str = f"{str(dest_target_dir).rstrip('/')}/{dag_task_group_identifier}/{run_id}"

        run_dir_path = ObjectStoragePath(run_dir_path_str, conn_id=dest_conn_id)

        if run_dir_path.exists():
            run_dir_path.rmdir(recursive=True)
