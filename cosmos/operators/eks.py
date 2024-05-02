import warnings
from typing import Sequence, Callable, Any

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from airflow.utils.context import Context, context_merge

from cosmos.dbt.parser.output import extract_log_issues, DBT_NO_TESTS_MSG, DBT_WARN_MSG
from cosmos.operators.base import DbtBuildMixin, DbtRunOperationMixin, DbtTestMixin, DbtRunMixin, DbtSnapshotMixin, \
    DbtSeedMixin, DbtLSMixin
from cosmos.operators.kubernetes import DbtKubernetesBaseOperator

CHECK_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 25 * 60
DEFAULT_COMPUTE_TYPE = "nodegroup"
DEFAULT_CONN_ID = "aws_default"
DEFAULT_FARGATE_PROFILE_NAME = "profile"
DEFAULT_NAMESPACE_NAME = "default"
DEFAULT_NODEGROUP_NAME = "nodegroup"


class DbtEksBaseOperator(DbtKubernetesBaseOperator):
    template_fields: Sequence[str] = tuple(
        {
            "cluster_name",
            "in_cluster",
            "namespace",
            "pod_name",
            "aws_conn_id",
            "region",
        } | set(DbtKubernetesBaseOperator.template_fields)
    )

    def __init__(
            self,
            cluster_name: str,
            # Setting in_cluster to False tells the pod that the config
            # file is stored locally in the worker and not in the cluster.
            in_cluster: bool = False,
            namespace: str = DEFAULT_NAMESPACE_NAME,
            pod_context: str | None = None,
            pod_name: str | None = None,
            pod_username: str | None = None,
            aws_conn_id: str = DEFAULT_CONN_ID,
            region: str | None = None,
            on_finish_action: str | None = None,
            **kwargs,
    ) -> None:
        if on_finish_action is not None:
            kwargs["on_finish_action"] = OnFinishAction(on_finish_action)
        else:
            warnings.warn(
                f"You have not set parameter `on_finish_action` in class {self.__class__.__name__}. "
                "Currently the default for this parameter is `keep_pod` but in a future release"
                " the default will be changed to `delete_pod`. To ensure pods are not deleted in"
                " the future you will need to set `on_finish_action=keep_pod` explicitly.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["on_finish_action"] = OnFinishAction.KEEP_POD

        self.cluster_name = cluster_name
        self.in_cluster = in_cluster
        self.namespace = namespace
        self.pod_name = pod_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(
            in_cluster=self.in_cluster,
            namespace=self.namespace,
            name=self.pod_name,
            **kwargs,
        )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the EksPodOperator.
        if self.config_file:
            raise AirflowException("The config_file is not an allowed parameter for the EksPodOperator.")

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        with eks_hook.generate_config_file(
                eks_cluster_name=self.cluster_name, pod_namespace=self.namespace
        ) as self.config_file:
            return super().execute(context)


class DbtBuildEksOperator(DbtBuildMixin, DbtEksBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[
        str] = DbtEksBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]


class DbtLSEksOperator(DbtLSMixin, DbtEksBaseOperator):
    """
    Executes a dbt core ls command.
    """


class DbtSeedEksOperator(DbtSeedMixin, DbtEksBaseOperator):
    """
    Executes a dbt core seed command.
    """

    template_fields: Sequence[
        str] = DbtEksBaseOperator.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]


class DbtSnapshotEksOperator(DbtSnapshotMixin, DbtEksBaseOperator):
    """
    Executes a dbt core snapshot command.
    """


class DbtRunEksOperator(DbtRunMixin, DbtEksBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[
        str] = DbtEksBaseOperator.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]


class DbtTestEksOperator(DbtTestMixin, DbtEksBaseOperator):
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
                and isinstance(context["task_instance"].task, DbtTestEksOperator)
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
                and isinstance(context["task_instance"].task, DbtTestEksOperator)
        ):
            return
        task = context["task_instance"].task
        if task.pod:
            task.on_finish_action = self.on_finish_action_original
            task.cleanup(pod=task.pod, remote_pod=task.remote_pod)


class DbtRunOperationEksOperator(DbtRunOperationMixin, DbtEksBaseOperator):
    """
    Executes a dbt core run-operation command.
    """

    template_fields: Sequence[
        str] = DbtEksBaseOperator.template_fields + DbtRunOperationMixin.template_fields  # type: ignore[operator]
