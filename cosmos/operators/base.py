from __future__ import annotations

import logging
import os
from typing import Any
import yaml

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

from cosmos.config import CosmosConfig

logger = logging.getLogger(__name__)


class DbtBaseOperator(BaseOperator):
    """
    Executes a dbt core cli command.

    :param cosmos_config: The project configuration
    :param base_cmd: dbt sub-command to run (i.e ls, seed, run, test, etc.)
    :param models: dbt optional argument that specifies which nodes to include.
    :param env: Environment variables to expose to the bash command. (templated)
    :param dbt_vars: Variables to pass to the dbt command as `--vars` arguments. (templated)
    """

    template_fields: list[str] = ["env", "dbt_vars"]
    base_cmd: list[str] = ["help"]

    def __init__(
        self,
        cosmos_config: CosmosConfig,
        models: list[str] | None = None,
        env: dict[str, Any] | None = None,
        dbt_vars: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.cosmos_config = cosmos_config
        self.models = models
        self.env = env or {}
        self.dbt_vars = dbt_vars or {}

        super().__init__(**kwargs)

    def get_env(self, context: Context) -> dict[str, str]:
        """
        Builds the set of environment variables to be exposed for the bash command.

        The order of determination is:
            1. If execution_config.append_env is True, the current process environment.
            2. The Airflow context as environment variables.
            3. The env parameter passed to the Operator

        Note that this also filters out any invalid types that cannot be cast to strings.
        """
        env: dict[str, Any] = {}

        if self.cosmos_config.execution_config.append_env:
            env.update(os.environ)

        # Airflow context as environment variables
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        # env parameter passed to the Operator
        if self.env and isinstance(self.env, dict):
            env.update(self.env)

        # turn all values into strings
        env = {key: str(val) for key, val in env.items()}
        return env

    def build_cmd(
        self,
        flags: list[str] | None = None,
    ) -> list[str]:
        """
        Builds the dbt command to be executed, by joining the following (in order):
        1. dbt executable path
        2. base command
        3. models argument
        4. vars argument
        5. command specific flags passed to this function from subclasses
        6. user-supplied flags from the execution config
        """
        dbt_cmd = [str(self.cosmos_config.execution_config.dbt_executable_path)]
        dbt_cmd.extend(self.base_cmd)

        # add models argument
        if self.models:
            dbt_cmd.extend(["--select", ",".join(self.models)])

        # add vars argument
        if self.dbt_vars:
            dbt_cmd.extend(["--vars", yaml.dump(self.dbt_vars)])

        # add command specific flags
        if flags:
            dbt_cmd.extend(flags)

        # add user-supplied flags
        dbt_cmd.extend(self.cosmos_config.execution_config.dbt_cli_flags)

        return dbt_cmd

    def execute(self, context: Context) -> None:
        raise NotImplementedError("Subclasses should implement this!")
