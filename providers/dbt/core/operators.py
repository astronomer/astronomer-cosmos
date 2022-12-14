from typing import Sequence

import os
import shutil

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

from cosmos.providers.dbt.core.utils.profiles_generator import create_default_profiles, map_profile


class DBTBaseOperator(BaseOperator):

    template_fields: Sequence[str] = ("env", "vars")
    ui_color = "#ed7254"

    def __init__(
        self,
        project_dir: str,
        conn_id: str,
        base_cmd: str = None,
        select: str = None,
        exclude: str = None,
        selector: str = None,
        vars: str = None,
        models: str = None,
        cache_selected_only: bool = False,
        no_version_check: bool = False,
        fail_fast: bool = False,
        quiet: bool = False,
        warn_error: bool = False,
        db_name: str = None,
        schema: str = None,
        env: dict = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int = 99,
        **kwargs,
    ) -> None:
        self.project_dir = project_dir
        self.conn_id = conn_id
        self.base_cmd = base_cmd
        self.select = select
        self.exclude = exclude
        self.selector = selector
        self.vars = vars
        self.models = models
        self.cache_selected_only = cache_selected_only
        self.no_version_check = no_version_check
        self.fail_fast = fail_fast
        self.quiet = quiet
        self.warn_error = warn_error
        self.db_name = db_name
        self.schema = schema
        self.env = env
        self.append_env = append_env
        self.output_encoding = output_encoding
        self.skip_exit_code = skip_exit_code
        super().__init__(**kwargs)

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def get_env(self, context):
        """Builds the set of environment variables to be exposed for the bash command."""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        else:
            if self.append_env:
                system_env.update(env)
                env = system_env

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting the following env vars:\n%s",
            "\n".join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)

        return env

    def get_dbt_path(self):
        dbt_path = shutil.which("dbt") or "dbt"
        if self.project_dir is not None:
            if not os.path.exists(self.project_dir):
                raise AirflowException(f"Can not find the project_dir: {self.project_dir}")
            if not os.path.isdir(self.project_dir):
                raise AirflowException(f"The project_dir {self.project_dir} must be a directory")
        return dbt_path

    def exception_handling(self, result):
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"dbt command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            raise AirflowException(f"dbt command failed. The command returned a non-zero exit code {result.exit_code}.")

    def add_global_flags(self):

        global_flags = [
            "project_dir",
            "select",
            "exclude",
            "selector",
            "vars",
            "models",
        ]

        flags = []
        for global_flag in global_flags:
            dbt_name = f"--{global_flag.replace('_', '-')}"
            global_flag_value = self.__getattribute__(global_flag)
            if global_flag_value is not None:
                flags.append(dbt_name)
                flags.append(str(global_flag_value))

        global_boolean_flags = [
            "no_version_check",
            "cache_selected_only",
            "fail_fast",
            "quiet",
            "warn_error",
        ]
        for global_boolean_flag in global_boolean_flags:
            dbt_name = f"--{global_boolean_flag.replace('_', '-')}"
            global_boolean_flag_value = self.__getattribute__(global_boolean_flag)
            if global_boolean_flag_value is True:
                flags.append(dbt_name)
        return flags

    def build_command(self):
        dbt_path = self.get_dbt_path()
        cmd = [dbt_path, self.base_cmd] + self.add_global_flags()
        return cmd

    def run_command(self, cmd, env):
        result = self.subprocess_hook.run_command(
            command=cmd,
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.project_dir,
        )
        self.exception_handling(result)
        return result

    def build_and_run_cmd(self, env):
        create_default_profiles()
        profile, profile_vars = map_profile(conn_id=self.conn_id, db_override=self.db_name, schema_override=self.schema)
        env = env | profile_vars
        cmd = self.build_command() + ["--profile", profile]
        result = self.run_command(cmd=cmd, env=env)
        return result

    def execute(self, context: Context):
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTLSOperator(DBTBaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def add_cmd_flags(self):
        flags = []
        return flags

    def execute(self, context: Context):
        self.base_cmd = "ls"
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTSeedOperator(DBTBaseOperator):
    def __init__(self, full_refresh: bool = False, **kwargs) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)

    def add_cmd_flags(self):
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context):
        self.base_cmd = "seed"
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTRunOperator(DBTBaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def add_cmd_flags(self):
        flags = []
        return flags

    def execute(self, context: Context):
        self.base_cmd = "run"
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTTestOperator(DBTBaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def add_cmd_flags(self):
        flags = []
        return flags

    def execute(self, context: Context):
        self.base_cmd = "test"
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output
