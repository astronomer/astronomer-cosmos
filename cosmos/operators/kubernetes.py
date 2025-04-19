from __future__ import annotations

import inspect
import re
from os import PathLike
from typing import Any, Callable, Sequence

from airflow.models import TaskInstance
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
from airflow.utils.context import Context, context_merge

from cosmos.config import ProfileConfig
from cosmos.dbt.parser.output import extract_log_issues
from cosmos.operators.base import (
    AbstractDbtBase,
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


class DbtKubernetesBaseOperator(AbstractDbtBase, KubernetesPodOperator):  # type: ignore
    """
    Executes a dbt core cli command in a Kubernetes Pod.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) + list(KubernetesPodOperator.template_fields)
    )

    intercept_flag = False

    def __init__(self, profile_config: ProfileConfig | None = None, **kwargs: Any) -> None:
        self.profile_config = profile_config
        super().__init__(**kwargs)
        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        base_operator_args = set(inspect.signature(KubernetesPodOperator.__init__).parameters.keys())
        base_kwargs = {}
        for arg_key, arg_value in kwargs.items():
            if arg_key in base_operator_args:
                base_kwargs[arg_key] = arg_value
        base_kwargs["task_id"] = kwargs["task_id"]
        KubernetesPodOperator.__init__(self, **base_kwargs)

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


class DbtTestWarningHandler(KubernetesPodOperatorCallback):
    def __init__(self, on_warning_callback: Callable[..., Any]) -> None:
        self.on_warning_callback = on_warning_callback

    def on_pod_completion(
            self,
            *,
            context: Context,
            operator: KubernetesPodOperator,
            **kwargs,
    ) -> None:
        """
        Handles warnings by extracting log issues, creating additional context, and calling the
        on_warning_callback with the updated context.

        :param context: The original airflow context in which the build and run command was executed.
        :param operator: The original pod operator instance.
        """
        if not (
            isinstance(context["task_instance"], TaskInstance)
            and (
                isinstance(context["task_instance"].task, DbtTestKubernetesOperator)
                or isinstance(context["task_instance"].task, DbtSourceKubernetesOperator)
            )
        ):
            return
        task = context["task_instance"].task
        logs = [
            log.decode("utf-8") for log in task.pod_manager.read_pod_logs(task.pod, "base") if
            log.decode("utf-8") != ""
        ]

        warn_count_pattern = re.compile(r'Done\. (?:\w+=\d )*WARN=(\d)(?: \w+=\d)*')
        warn_count = warn_count_pattern.search('\n'.join(logs))
        if not warn_count:
            operator.log.warning(
                "Failed to scrape warning count from the pod logs. Potential warning callbacks could not be triggered."
            )
            return

        if int(warn_count.group(1)) > 0:
            test_names, test_results = extract_log_issues(logs)
            context_merge(context, test_names=test_names, test_results=test_results)
            self.on_warning_callback(context)


class DbtTestKubernetesOperator(DbtTestMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if on_warning_callback:
            test_warning_handler = DbtTestWarningHandler(on_warning_callback)
            self.callbacks += [test_warning_handler]


class DbtSourceKubernetesOperator(DbtSourceMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if on_warning_callback:
            test_warning_handler = DbtTestWarningHandler(on_warning_callback)
            self.callbacks += [test_warning_handler]


class DbtRunKubernetesOperator(DbtRunMixin, DbtKubernetesBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtKubernetesBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


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
