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
    """
    Executes a dbt core cli command.

    :param project_dir: Which directory to look in for the dbt_project.yml file. Default is the current working
    directory and its parents.
    :param conn_id: The airflow connection to use as the target
    :param base_cmd: dbt sub-command to run (i.e ls, seed, run, test, etc.)
    :param select: dbt optional argument that specifies which nodes to include.
    :param exclude: dbt optional argument that specifies which models to exclude.
    :param selector: dbt optional argument - the selector name to use, as defined in selectors.yml
    :param vars: dbt optional argument - Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}' (templated)
    :param models: dbt optional argument that specifies which nodes to include.
    :param cache_selected_only:
    :param no_version_check: dbt optional argument - If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
    :param fail_fast: dbt optional argument to make dbt exit immediately if a single resource fails to build.
    :param quiet: dbt optional argument to show only error logs in stdout
    :param warn_error: dbt optional argument to convert dbt warnings into errors
    :param db_name: override the target db instead of the one supplied in the airflow connection
    :param schema: override the target schema instead of the one supplied in the airflow connection
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :param append_env: If False(default) uses the environment variables passed in env params
        and does not inherit the current process environment. If True, inherits the environment variables
        from current passes and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it
    :param output_encoding: Output encoding of bash command
    :param skip_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param python_venv: Path to venv for dbt command execution (i.e. /home/astro/.pyenv/versions/dbt_venv/bin/activate)
    """

    template_fields: Sequence[str] = ("env", "vars")
    ui_color = "#9370DB"

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
        python_venv: str = None,
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
        self.python_venv = python_venv
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
        bash_path = shutil.which("bash") or "bash"

        if self.python_venv:
            dbt_path = [bash_path, "-c", f"source {self.python_venv} && dbt {self.base_cmd} "]
        else:
            dbt_path = [bash_path, "-c", f"dbt {self.base_cmd} "]

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

    def run_command(self, cmd, env):
        result = self.subprocess_hook.run_command(
            command=cmd,
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.project_dir,
        )
        self.exception_handling(result)
        return result

    def build_and_run_cmd(self, env: dict, cmd_flags: list = None):
        create_default_profiles()
        profile, profile_vars = map_profile(conn_id=self.conn_id, db_override=self.db_name, schema_override=self.schema)

        # parse dbt command

        ## get the dbt piece from the bash command so we can add to it
        dbt_path = self.get_dbt_path()
        dbt_cmd = dbt_path[-1]
        dbt_path.pop()

        ## add global flags
        for item in self.add_global_flags():
            dbt_cmd += f"{item} "

        ## add command specific flags
        if cmd_flags:
            for item in cmd_flags:
                dbt_cmd += f"{item} "

        ## add profile
        dbt_cmd += f"--profile {profile}"

        ## and appended dbt command back to bash
        dbt_path.append(dbt_cmd)
        cmd = dbt_path

        ## set env vars
        env = env | profile_vars
        result = self.run_command(cmd=cmd, env=env)
        return result

    def execute(self, context: Context):
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTLSOperator(DBTBaseOperator):
    """
    Executes a dbt core ls command.

    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTSeedOperator(DBTBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models

    """

    def __init__(self, full_refresh: bool = False, **kwargs) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = "seed"

    def add_cmd_flags(self):
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context):
        cmd_flags = self.add_cmd_flags()
        result = self.build_and_run_cmd(env=self.get_env(context), cmd_flags=cmd_flags)
        return result.output


class DBTRunOperator(DBTBaseOperator):
    """
    Executes a dbt core run command.

    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "run"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output


class DBTTestOperator(DBTBaseOperator):
    """
    Executes a dbt core test command.

    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        result = self.build_and_run_cmd(env=self.get_env(context))
        return result.output
