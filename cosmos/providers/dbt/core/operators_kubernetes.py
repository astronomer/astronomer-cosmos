from __future__ import annotations

import os
from typing import Any, Dict, List, Sequence

import yaml
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.subprocess import SubprocessHook
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

from cosmos.providers.dbt.core.utils.profiles_generator import conn_exists, map_profile


class DbtKubernetesBaseOperator(KubernetesPodOperator):
    """
    Executes a dbt core cli command.

    :param project_dir: Which directory to look in for the dbt_project.yml file. Default is the current working
    directory and its parents.
    :type project_dir: str
    :param conn_id: The airflow connection to use as the target
    :type conn_id: str
    :param base_cmd: dbt sub-command to run (i.e ls, seed, run, test, etc.)
    :type base_cmd: str | list[str]
    :param select: dbt optional argument that specifies which nodes to include.
    :type select: str
    :param exclude: dbt optional argument that specifies which models to exclude.
    :type exclude: str
    :param selector: dbt optional argument - the selector name to use, as defined in selectors.yml
    :type selector: str
    :param vars: dbt optional argument - Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}' (templated)
    :type vars: dict
    :param models: dbt optional argument that specifies which nodes to include.
    :type models: str
    :param cache_selected_only:
    :type cache_selected_only: bool
    :param no_version_check: dbt optional argument - If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
    :type no_version_check: bool
    :param fail_fast: dbt optional argument to make dbt exit immediately if a single resource fails to build.
    :type fail_fast: bool
    :param quiet: dbt optional argument to show only error logs in stdout
    :type quiet: bool
    :param warn_error: dbt optional argument to convert dbt warnings into errors
    :type warn_error: bool
    :param db_name: override the target db instead of the one supplied in the airflow connection
    :type db_name: str
    :param schema: override the target schema instead of the one supplied in the airflow connection
    :type schema: str
    :param env: If env is not None, it must be a dict that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param append_env: If False(default) uses the environment variables passed in env params
        and does not inherit the current process environment. If True, inherits the environment variables
        from current passes and then environment variable passed by the user will either update the existing
        inherited environment variables or the new variables gets appended to it
    :type append_env: bool
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str
    :param skip_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: 99). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :type skip_exit_code: int
    :param cancel_query_on_kill: If true, then cancel any running queries when the task's on_kill() is executed.
        Otherwise, the query will keep running when the task is killed.
    :type cancel_query_on_kill: bool
    :param dbt_executable_path: Path to dbt executable can be used with venv
        (i.e. /home/astro/.pyenv/versions/dbt_venv/bin/dbt)
    :type dbt_executable_path: str
    """

    template_fields: Sequence[str] = (
        "env",
        "vars",
    ) + KubernetesPodOperator.template_fields

    def __init__(
        self,
        project_dir: str,
        conn_id: str,
        base_cmd: str | List[str] = None,
        select: str = None,
        exclude: str = None,
        selector: str = None,
        vars: dict = None,
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
        cancel_query_on_kill: bool = True,
        dbt_executable_path: str = "dbt",
        container_flags: Dict[str, Any] = {},
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
        self.cancel_query_on_kill = cancel_query_on_kill
        self.dbt_executable_path = dbt_executable_path
        self.container_flags = container_flags
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

    def exception_handling(self, result):
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(
                f"dbt command returned exit code {self.skip_exit_code}. Skipping."
            )
        elif result.exit_code != 0:
            raise AirflowException(
                f"dbt command failed. The command returned a non-zero exit code {result.exit_code}."
            )

    def add_global_flags(self):
        global_flags = [
            "project_dir",
            "profiles_dir",
            "select",
            "exclude",
            "selector",
            "vars",
            "models",
        ]

        flags = []
        for global_flag in global_flags:
            dbt_name = f"--{global_flag.replace('_', '-')}"

            global_flag_value = self.container_flags.get(global_flag)
            if global_flag_value is None:
                global_flag_value = self.__getattribute__(global_flag)

            if global_flag_value is not None:
                if isinstance(global_flag_value, dict):
                    # handle dict
                    yaml_string = yaml.dump(global_flag_value)
                    flags.append(dbt_name)
                    flags.append(yaml_string)
                else:
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

    def build_env_args(self, env: dict):
        env_vars_dict = {}
        for env_var in self.env_vars:
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars = convert_env_vars({**env, **env_vars_dict})

    def build_cmd(self, env: dict, cmd_flags: list = None):
        profile_vars = {}
        if conn_exists(conn_id=self.conn_id):
            _, profile_vars = map_profile(
                conn_id=self.conn_id,
                db_override=self.db_name,
                schema_override=self.schema,
            )

        # parse dbt args
        dbt_args = []

        ## start with the dbt executable
        self.cmds = [self.dbt_executable_path]

        ## add base cmd
        if isinstance(self.base_cmd, str):
            dbt_args.append(self.base_cmd)
        else:
            [dbt_args.append(item) for item in self.base_cmd]

        # add global flags
        for item in self.add_global_flags():
            dbt_args.append(item)

        ## add command specific flags
        if cmd_flags:
            for item in cmd_flags:
                dbt_args.append(item)

        ## set env vars
        self.build_env_args({**env, **profile_vars})

        self.arguments = dbt_args


class DbtLSOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.

    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        self.build_cmd(env=self.get_env(context))
        return super().execute(context)


class DbtSeedOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models

    """

    ui_color = "#F58D7E"

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
        self.build_cmd(env=self.get_env(context), cmd_flags=cmd_flags)
        return super().execute(context)


class DbtRunOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.

    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "run"

    def execute(self, context: Context):
        self.build_cmd(env=self.get_env(context))
        return super().execute(context)


class DbtTestOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.

    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        self.build_cmd(env=self.get_env(context))
        return super().execute(context)


class DbtRunOperationOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :type macro_name: str
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    :type args: dict
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = "args"

    def __init__(self, macro_name: str, args: dict = None, **kwargs) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)
        self.base_cmd = ["run-operation", macro_name]

    def add_cmd_flags(self):
        flags = []
        if self.args is not None:
            flags.append("--args")
            flags.append(yaml.dump(self.args))
        return flags

    def execute(self, context: Context):
        cmd_flags = self.add_cmd_flags()
        self.build_cmd(env=self.get_env(context), cmd_flags=cmd_flags)
        return super().execute(context)


class DbtDepsOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core deps command.

    :param vars: Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file
    :type vars: dict
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "deps"

    def execute(self, context: Context):
        self.build_cmd(env=self.get_env(context))
        return super().execute(context)
