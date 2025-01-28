from __future__ import annotations

from os import PathLike
from typing import Any, Callable, Sequence

from airflow.models import TaskInstance
from airflow.utils.context import Context, context_merge

from cosmos.config import ProfileConfig
from cosmos.dbt.parser.output import extract_log_issues
from cosmos.operators.base import (
    AbstractDbtBaseOperator,
    DbtBuildMixin,
    DbtCloneMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

DBT_NO_TESTS_MSG = "Nothing to do"
DBT_WARN_MSG = "WARN"

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
    except ImportError:
        raise ImportError(
            "Could not import KubernetesPodOperator. Ensure you've installed the Kubernetes provider "
            "separately or with with `pip install astronomer-cosmos[...,kubernetes]`."
        )


class DbtKubernetesBaseOperator(AbstractDbtBaseOperator, KubernetesPodOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBaseOperator.template_fields) + list(KubernetesPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        self.profile_config = profile_config
        super().__init__(**kwargs)

    def build_env_args(self, env: dict[str, str | bytes | PathLike[Any]]) -> None:
        env_vars_dict: dict[str, str] = dict()

        for env_var_key, env_var_value in env.items():
            env_vars_dict[env_var_key] = str(env_var_value)
        for env_var in self.env_vars:
            env_vars_dict[env_var.name] = env_var.value

        self.env_vars: list[Any] = convert_env_vars(env_vars_dict)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        self.build_kube_args(context, cmd_flags)
        self.log.info(f"Running command: {self.arguments}")
        result = KubernetesPodOperator.execute(self, context)
        self.log.info(result)

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


class DbtBuildKubernetesOperator(DbtBuildMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSKubernetesOperator(DbtLSMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedKubernetesOperator(DbtSeedMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotKubernetesOperator(DbtSnapshotMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceKubernetesOperator(DbtSourceMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunKubernetesOperator(DbtRunMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestKubernetesOperator(DbtTestMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.
    """

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


class DbtRunOperationKubernetesOperator(DbtRunOperationMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run-operation command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtCloneKubernetesOperator(DbtCloneMixin, DbtKubernetesBaseOperator):
    """Executes a dbt core clone command."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
