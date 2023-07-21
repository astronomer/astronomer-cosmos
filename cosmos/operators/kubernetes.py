from __future__ import annotations

import logging
from os import PathLike
from typing import Any, Callable, Sequence

import yaml
from airflow.utils.context import Context

from cosmos.operators.base import DbtBaseOperator

logger = logging.getLogger(__name__)

# kubernetes is an optional dependency, so we need to check if it's installed
try:
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
        convert_env_vars,
    )
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:
    raise ImportError(
        "Could not import KubernetesPodOperator. Ensure you've installed the Kubernetes provider "
        "separately or with with `pip install astronomer-cosmos[...,kubernetes]`."
    )


class DbtKubernetesBaseOperator(KubernetesPodOperator, DbtBaseOperator):  # type: ignore[misc]
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = DbtBaseOperator.template_fields + KubernetesPodOperator.template_fields

    intercept_flag = False

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def build_env_args(self, env: dict[str, str | bytes | PathLike[Any]]) -> None:
        env_vars_dict = dict()

        for env_var in self.env_vars:  # type: ignore[has-type]
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars = convert_env_vars({**env, **env_vars_dict})

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> Any:
        self.build_kube_args(cmd_flags, context)
        self.log.info(f"Running command: {self.arguments}")
        return super().execute(context)

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)
        # set env vars
        self.build_env_args(env_vars)
        self.arguments = dbt_cmd


class DbtLSKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["ls"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtSeedKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"

    def __init__(self, full_refresh: bool = False, **kwargs: str) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = ["seed"]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context) -> Any:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtSnapshotKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core snapshot command.

    """

    ui_color = "#964B00"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["snapshot"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtRunKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["run"]

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtTestKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.
    """

    ui_color = "#8194E0"

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["test"]
        # as of now, on_warning_callback in kubernetes executor does nothing
        self.on_warning_callback = on_warning_callback

    def execute(self, context: Context) -> Any:
        return self.build_and_run_cmd(context=context)


class DbtRunOperationKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = ("args",)

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: str) -> None:
        self.macro_name = macro_name
        self.args = args
        super().__init__(**kwargs)
        self.base_cmd = ["run-operation", macro_name]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.args is not None:
            flags.append("--args")
            flags.append(yaml.dump(self.args))
        return flags

    def execute(self, context: Context) -> Any:
        cmd_flags = self.add_cmd_flags()
        return self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
