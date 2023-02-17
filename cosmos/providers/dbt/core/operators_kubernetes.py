from __future__ import annotations

from typing import Sequence

import yaml
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.context import Context

from cosmos.providers.dbt.core.operators import DbtBaseOperator


class DbtKubernetesBaseOperator(KubernetesPodOperator, DbtBaseOperator):
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = (
        DbtBaseOperator.template_fields + KubernetesPodOperator.template_fields
    )

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def build_env_args(self, env: dict):
        env_vars_dict = {}
        for env_var in self.env_vars:
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars = convert_env_vars({**env, **env_vars_dict})

    def build_cmd_and_args(self, env: dict, cmd_flags: list = None):
        dbt_cmd, env_vars = self.build_cmd(
            env=env, cmd_flags=cmd_flags, handle_profile=False
        )

        ## set env vars
        self.build_env_args(env_vars)

        self.arguments = dbt_cmd
        self.log.info(f"Building command: {self.arguments}")


class DbtLSKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.

    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "ls"

    def execute(self, context: Context):
        self.build_cmd_and_args(env=self.get_env(context))
        return super().execute(context)


class DbtSeedKubernetesOperator(DbtKubernetesBaseOperator):
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
        self.build_cmd_and_args(env=self.get_env(context), cmd_flags=cmd_flags)
        return super().execute(context)


class DbtRunKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.

    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "run"

    def execute(self, context: Context):
        self.build_cmd_and_args(env=self.get_env(context))
        return super().execute(context)


class DbtTestKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.

    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_cmd = "test"

    def execute(self, context: Context):
        self.build_cmd_and_args(env=self.get_env(context))
        return super().execute(context)


class DbtRunOperationKubernetesOperator(DbtKubernetesBaseOperator):
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
        self.build_cmd_and_args(env=self.get_env(context), cmd_flags=cmd_flags)
        return super().execute(context)


class DbtDepsKubernetesOperator(DbtKubernetesBaseOperator):
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
        self.build_cmd_and_args(env=self.get_env(context))
        return super().execute(context)
