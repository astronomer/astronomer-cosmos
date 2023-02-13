from __future__ import annotations

import os
from typing import List, Dict, Any, Sequence

import yaml
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars

from cosmos.providers.dbt.core.utils.profiles_generator import (
    create_default_profiles,
    map_profile,
)


class DbtBaseOperator(BaseOperator):
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
    :param dbt_cmd_flags: Flags passed to dbt command override those that are calculated.
    :type dbt_cmd_flags: dict
    """

    template_fields: Sequence[str] = ("env", "vars")

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
        dbt_cmd_flags: Dict[str, Any] = {},
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
        self.dbt_cmd_flags = dbt_cmd_flags
        super().__init__(**kwargs)

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
            
            global_flag_value = self.dbt_cmd_flags.get(global_flag)
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
            
            global_boolean_flag_value = self.dbt_cmd_flags.get(global_boolean_flag)
            if global_boolean_flag_value is None:
                global_boolean_flag_value = self.__getattribute__(global_boolean_flag)
            
            if global_boolean_flag_value is True:
                flags.append(dbt_name)
        return flags

    def build_cmd(self, env: dict, cmd_flags: list = None, handle_profile: bool = True):
        # parse dbt command
        dbt_cmd = []

        ## start with the dbt executable
        dbt_cmd.append(self.dbt_executable_path)

        ## add base cmd
        if isinstance(self.base_cmd, str):
            dbt_cmd.append(self.base_cmd)
        else:
            [dbt_cmd.append(item) for item in self.base_cmd]

        # add global flags
        for item in self.add_global_flags():
            dbt_cmd.append(item)

        ## add command specific flags
        if cmd_flags:
            for item in cmd_flags:
                dbt_cmd.append(item)

        ## add profile
        if handle_profile:            
            create_default_profiles()        
            profile, profile_vars = map_profile(
                conn_id=self.conn_id, db_override=self.db_name, schema_override=self.schema
            )
            
            dbt_cmd.append("--profile")
            dbt_cmd.append(profile)           
                
            ## set env vars
            env = {**env, **profile_vars}

        return dbt_cmd, env