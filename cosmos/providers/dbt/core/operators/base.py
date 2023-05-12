from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Dict, Sequence, Tuple

import yaml
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

logger = logging.getLogger(__name__)


class DbtBaseOperator(BaseOperator):
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
    :param cancel_query_on_kill: If true, then cancel any running queries when the task's on_kill() is executed.
        Otherwise, the query will keep running when the task is killed.
    :param dbt_executable_path: Path to dbt executable can be used with venv
        (i.e. /home/astro/.pyenv/versions/dbt_venv/bin/dbt)
    :param dbt_cmd_flags: Flags passed to dbt command override those that are calculated.
    """

    template_fields: Sequence[str] = ("env", "vars")
    global_flags = (
        "project_dir",
        "select",
        "exclude",
        "selector",
        "vars",
        "models",
    )
    global_boolean_flags = (
        "no_version_check",
        "cache_selected_only",
        "fail_fast",
        "quiet",
        "warn_error",
    )

    include_system_env = False
    intercept_flag = True

    def __init__(
        self,
        project_dir: str,
        conn_id: str,
        base_cmd: str | list[str] = None,
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
        # dbt-ol is the OpenLineage wrapper for dbt, which automatically
        # generates and emits lineage data to a specified backend.
        dbt_ol_path = shutil.which("dbt-ol")
        if dbt_executable_path == "dbt" and shutil.which("dbt-ol"):
            self.dbt_executable_path = dbt_ol_path
        elif dbt_executable_path == "dbt":
            self.dbt_executable_path = shutil.which("dbt")
        else:
            self.dbt_executable_path = dbt_executable_path
        self.dbt_cmd_flags = dbt_cmd_flags
        super().__init__(**kwargs)

    def get_env(self, context: Context, profile_vars: dict[str, str]) -> dict[str, str]:
        """
        Builds the set of environment variables to be exposed for the bash command.
        The order of determination is:
            1. The env parameter passed to the Operator
            2. The Airflow context as environment variables.
            3. The profile variables from the dbt profile file.
        If a user accidentally uses a key that is found earlier in the determination order then it is overwritten.
        """
        env: dict[str, Any] = {}

        # env parameter passed to the Operator
        if self.env and isinstance(self.env, dict):
            env.update(self.env)

        # Airflow context as environment variables
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        # profile variables from the dbt profile file
        env.update(profile_vars)

        # filter out invalid types
        accepted_types = (str, bytes, os.PathLike)
        filtered_env = {
            k: v
            for k, v in env.items()
            if all((isinstance(k, accepted_types), isinstance(v, accepted_types)))
        }

        return filtered_env

    def add_global_flags(self) -> list[str]:
        flags = []
        for global_flag in self.global_flags:
            # for now, skip the project_dir flag
            if global_flag == "project_dir":
                continue

            dbt_name = f"--{global_flag.replace('_', '-')}"
            global_flag_value = self.__getattribute__(global_flag)
            if global_flag_value is not None:
                if isinstance(global_flag_value, dict):
                    yaml_string = yaml.dump(global_flag_value)
                    flags.extend([dbt_name, yaml_string])
                else:
                    flags.extend([dbt_name, str(global_flag_value)])
        for global_boolean_flag in self.global_boolean_flags:
            if self.__getattribute__(global_boolean_flag):
                flags.append(f"--{global_boolean_flag.replace('_', '-')}")
        return flags

    def build_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
    ) -> Tuple[list[str], dict]:
        dbt_cmd = [self.dbt_executable_path]
        if isinstance(self.base_cmd, str):
            dbt_cmd.append(self.base_cmd)
        else:
            dbt_cmd.extend(self.base_cmd)
        dbt_cmd.extend(self.add_global_flags())
        # add command specific flags
        if cmd_flags:
            dbt_cmd.extend(cmd_flags)

        env = self.get_env(context, profile_vars={})

        return dbt_cmd, env
