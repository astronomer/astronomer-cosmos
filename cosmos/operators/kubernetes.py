from __future__ import annotations

import inspect
import re
from abc import ABC
from os import PathLike
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

import kubernetes.client as k8s
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import context_merge

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    # apache-airflow-providers-cncf-kubernetes >= 7.14.0
    from airflow.providers.cncf.kubernetes.callbacks import (
        KubernetesPodOperatorCallback,
    )
except ImportError:

    class KubernetesPodOperatorCallback:  # type: ignore[no-redef]
        """Mock fallback for older versions. Should not be used in practice."""

        pass


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
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2


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

        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}
        operator_args: set[str] = set()
        for clazz in KubernetesPodOperator.__mro__:
            operator_args.update(inspect.signature(clazz.__init__).parameters.keys())
            if clazz == BaseOperator:
                break
        for arg in operator_args:
            try:
                operator_kwargs[arg] = kwargs[arg]
            except KeyError:
                pass

        base_kwargs = {}
        for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
            try:
                base_kwargs[arg] = kwargs[arg]
            except KeyError:
                try:
                    base_kwargs[arg] = default_args[arg]
                except KeyError:
                    pass

        AbstractDbtBase.__init__(self, **base_kwargs)
        KubernetesPodOperator.__init__(self, **operator_kwargs)

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


class DbtTestWarningHandler(KubernetesPodOperatorCallback):  # type: ignore[misc]
    def __init__(
        self,
        on_warning_callback: Callable[..., Any],
        operator: KubernetesPodOperator,
        context: Optional[Context] = None,
    ) -> None:
        self.on_warning_callback = on_warning_callback
        self.operator = operator
        self.context = context

    def on_pod_completion(  # type: ignore[override]
        self,
        *,
        pod: k8s.V1Pod,
        **kwargs: Any,
    ) -> None:
        """
        Handles warnings by extracting log issues, creating additional context, and calling the
        on_warning_callback with the updated context.

        Note that the signature of the interface method changed in `cncf.kubernetes` provider version 10.2.0, where the
        `operator` and `context` parameters were added to all operator callbacks. To maintain forward compatibility
        with `cncf.kubernetes` provider version 7.14.0 and later, we pass these parameters via instance variables.

        :param pod: the created pod.
        """
        if not self.context:
            self.operator.log.warning("No context provided to the DbtTestWarningHandler.")
            return

        task = self.context["task_instance"].task
        if not (isinstance(task, DbtTestKubernetesOperator) or isinstance(task, DbtSourceKubernetesOperator)):
            self.operator.log.warning(f"Cannot handle dbt warnings for task of type {type(task)}.")
            return

        logs = [log.decode("utf-8") for log in task.pod_manager.read_pod_logs(pod, "base") if log.decode("utf-8") != ""]

        warn_count_pattern = re.compile(r"Done\. (?:\w+=\d+ )*WARN=(\d+)(?: \w+=\d+)*")
        warn_count = warn_count_pattern.search("\n".join(logs))
        if not warn_count:
            self.operator.log.warning(
                "Failed to scrape warning count from the pod logs."
                "Potential warning callbacks could not be triggered."
            )
            return

        if int(warn_count.group(1)) > 0:
            test_names, test_results = extract_log_issues(logs)
            context_merge(self.context, test_names=test_names, test_results=test_results)
            self.on_warning_callback(self.context)


class DbtWarningKubernetesOperator(DbtKubernetesBaseOperator, ABC):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, on_warning_callback: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.warning_handler = None
        if on_warning_callback:
            self.warning_handler = DbtTestWarningHandler(on_warning_callback, operator=self)
            # Support for handling multiple operator callbacks via self.callbacks was added in provider version 10.2.0
            if isinstance(self.callbacks, list):  # type: ignore[has-type]
                self.callbacks.append(self.warning_handler)  # type: ignore[has-type,arg-type]
            else:
                self.callbacks = self.warning_handler  # type: ignore[assignment]

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        if self.warning_handler:
            self.warning_handler.context = context
        super().build_and_run_cmd(
            context=context, cmd_flags=cmd_flags, run_as_async=run_as_async, async_context=async_context
        )


class DbtTestKubernetesOperator(DbtTestMixin, DbtWarningKubernetesOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceKubernetesOperator(DbtSourceMixin, DbtWarningKubernetesOperator):
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
