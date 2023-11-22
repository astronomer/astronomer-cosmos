from __future__ import annotations

from os import PathLike
from typing import Any, Callable, Sequence

import yaml
from airflow.utils.context import Context, context_merge

from cosmos.log import get_logger
from cosmos.config import ProfileConfig
from cosmos.operators.base import DbtBaseOperator

from airflow.models import TaskInstance
from cosmos.dbt.parser.output import extract_log_issues

DBT_NO_TESTS_MSG = "Nothing to do"
DBT_WARN_MSG = "WARN"

logger = get_logger(__name__)

try:
    # apache-airflow-providers-cncf-kubernetes >= 7.4.0
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
        convert_env_vars,
    )
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
except ImportError:
    try:
        # apache-airflow-providers-cncf-kubernetes < 7.4.0
        from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
    except ImportError as error:
        logger.exception(error)
        raise ImportError(
            "Could not import KubernetesPodOperator. Ensure you've installed the Kubernetes provider "
            "separately or with with `pip install astronomer-cosmos[...,kubernetes]`."
        )


class DbtKubernetesBaseOperator(KubernetesPodOperator, DbtBaseOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = tuple(
        list(DbtBaseOperator.template_fields) + list(KubernetesPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        self.profile_config = profile_config
        super().__init__(**kwargs)

    def build_env_args(self, env: dict[str, str | bytes | PathLike[Any]]) -> None:
        env_vars_dict: dict[str, str] = dict()

        for env_var in self.env_vars:
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars: list[Any] = convert_env_vars({**env, **env_vars_dict})

    def build_and_run_cmd(self, context: Context, cmd_flags: list[str] | None = None) -> Any:
        self.build_kube_args(context, cmd_flags)
        self.log.info(f"Running command: {self.arguments}")
        result = super().execute(context)
        logger.info(result)

    def build_kube_args(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        # For the first round, we're going to assume that the command is dbt
        # This means that we don't have openlineage support, but we will create a ticket
        # to add that in the future
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(context=context, cmd_flags=cmd_flags)

        # Parse ProfileConfig and add additional arguments to the dbt_cmd
        if self.profile_config:
            if self.profile_config.profile_name:
                dbt_cmd.extend(["--profile", self.profile_config.profile_name])
            if self.profile_config.target_name:
                dbt_cmd.extend(["--target", self.profile_config.target_name])

        if self.project_dir:
            dbt_cmd.extend(["--project-dir", str(self.project_dir)])

        # set env vars
        self.build_env_args(env_vars)
        self.arguments = dbt_cmd

    def execute(self, context: Context) -> None:
        self.build_and_run_cmd(context=context)


class DbtLSKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.
    """

    ui_color = "#DBCDF6"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["ls"]


class DbtSeedKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    ui_color = "#F58D7E"

    def __init__(self, full_refresh: bool = False, **kwargs: Any) -> None:
        self.full_refresh = full_refresh
        super().__init__(**kwargs)
        self.base_cmd = ["seed"]

    def add_cmd_flags(self) -> list[str]:
        flags = []
        if self.full_refresh is True:
            flags.append("--full-refresh")

        return flags

    def execute(self, context: Context) -> None:
        cmd_flags = self.add_cmd_flags()
        self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)


class DbtSnapshotKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core snapshot command.

    """

    ui_color = "#964B00"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["snapshot"]


class DbtRunKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.
    """

    ui_color = "#7352BA"
    ui_fgcolor = "#F4F2FC"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["run"]


class DbtTestKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.
    """

    ui_color = "#8194E0"

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        if not on_warning_callback:
            super().__init__(**kwargs)
        else:
            self.on_warning_callback = on_warning_callback
            self.is_delete_operator_pod_original = kwargs.get("is_delete_operator_pod", None)
            if self.is_delete_operator_pod_original is not None:
                self.on_finish_action_original = (
                    OnFinishAction.DELETE_POD if self.is_delete_operator_pod_original else OnFinishAction.KEEP_POD
                )
            else:
                self.on_finish_action_original = OnFinishAction(kwargs.get("on_finish_action", "delete_pod"))
                self.is_delete_operator_pod_original = self.on_finish_action_original == OnFinishAction.DELETE_POD
            # In order to read the pod logs, we need to keep the pod around.
            # Depending on the on_finish_action & is_delete_operator_pod settings,
            # we will clean up the pod later in the _handle_warnings method, which
            # is called in on_success_callback.
            kwargs["is_delete_operator_pod"] = False
            kwargs["on_finish_action"] = OnFinishAction.KEEP_POD

            # Add an additional callback to both success and failure callbacks.
            # In case of success, check for a warning in the logs and clean up the pod.
            self.on_success_callback = kwargs.get("on_success_callback", None) or []
            if isinstance(self.on_success_callback, list):
                self.on_success_callback += [self._handle_warnings]
            else:
                self.on_success_callback = [self.on_success_callback, self._handle_warnings]
            kwargs["on_success_callback"] = self.on_success_callback
            # In case of failure, clean up the pod.
            self.on_failure_callback = kwargs.get("on_failure_callback", None) or []
            if isinstance(self.on_failure_callback, list):
                self.on_failure_callback += [self._cleanup_pod]
            else:
                self.on_failure_callback = [self.on_failure_callback, self._cleanup_pod]
            kwargs["on_failure_callback"] = self.on_failure_callback

            super().__init__(**kwargs)

        self.base_cmd = ["test"]

    def _handle_warnings(self, context: Context) -> None:
        """
        Handles warnings by extracting log issues, creating additional context, and calling the
        on_warning_callback with the updated context.

        :param context: The original airflow context in which the build and run command was executed.
        """
        if not (
            isinstance(context["task_instance"], TaskInstance)
            and isinstance(context["task_instance"].task, DbtTestKubernetesOperator)
        ):
            return
        task = context["task_instance"].task
        logs = [
            log.decode("utf-8") for log in task.pod_manager.read_pod_logs(task.pod, "base") if log.decode("utf-8") != ""
        ]

        should_trigger_callback = all(
            [
                logs,
                self.on_warning_callback,
                DBT_NO_TESTS_MSG not in logs[-1],
                DBT_WARN_MSG in logs[-1],
            ]
        )

        if should_trigger_callback:
            warnings = int(logs[-1].split(f"{DBT_WARN_MSG}=")[1].split()[0])
            if warnings > 0:
                test_names, test_results = extract_log_issues(logs)
                context_merge(context, test_names=test_names, test_results=test_results)
                self.on_warning_callback(context)

        self._cleanup_pod(context)

    def _cleanup_pod(self, context: Context) -> None:
        """
        Handles the cleaning up of the pod after success or failure, if
        there is a on_warning_callback function defined.

        :param context: The original airflow context in which the build and run command was executed.
        """
        if not (
            isinstance(context["task_instance"], TaskInstance)
            and isinstance(context["task_instance"].task, DbtTestKubernetesOperator)
        ):
            return
        task = context["task_instance"].task
        if task.pod:
            task.on_finish_action = self.on_finish_action_original
            task.cleanup(pod=task.pod, remote_pod=task.remote_pod)


class DbtRunOperationKubernetesOperator(DbtKubernetesBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    ui_color = "#8194E0"
    template_fields: Sequence[str] = ("args",)

    def __init__(self, macro_name: str, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
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

    def execute(self, context: Context) -> None:
        cmd_flags = self.add_cmd_flags()
        self.build_and_run_cmd(context=context, cmd_flags=cmd_flags)
