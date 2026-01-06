from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pendulum import DateTime

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

import kubernetes.client as k8s
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback, client_type

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, _store_dbt_resource_status_from_log
from cosmos.operators.base import (
    DbtRunMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
)
from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSourceKubernetesOperator,
)

logger = get_logger(__name__)


# This global variable is currently used to make the task context available to the K8s callback.
# While the callback is set during the operator initialization, the context is only created during the operator's execution.
producer_task_context = None


class WatcherKubernetesCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]

    @staticmethod
    def progress_callback(
        *,
        line: str,
        client: client_type,
        mode: str,
        container_name: str,
        timestamp: DateTime | None,
        pod: k8s.V1Pod,
        **kwargs: Any,
    ) -> None:
        """
        Invoke this callback to process pod container logs.

        :param line: the read line of log.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        :param container_name: the name of the container from which the log line was read.
        :param timestamp: the timestamp of the log line.
        :param pod: the pod from which the log line was read.
        """
        if "context" not in kwargs:
            # This global variable is used to make the task context available to the K8s callback.
            # While the callback is set during the operator initialization, the context is only created during the operator's execution.
            kwargs["context"] = producer_task_context
        _store_dbt_resource_status_from_log(line, kwargs)


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):

    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = _store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")

        # Disable retries on producer task
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0

        super().__init__(task_id=task_id, *args, callbacks=WatcherKubernetesCallback, **kwargs)
        self.dbt_cmd_flags += ["--log-format", "json"]

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return CosmosKubernetesPodManager(kube_client=self.client, callbacks=self.callbacks)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException(
                "DbtProducerWatcherKubernetesOperator expects a task instance in the execution context"
            )

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            retry_message = (
                "DbtProducerWatcherKubernetesOperator does not support Airflow retries. "
                f"Detected attempt #{try_number}; failing fast to avoid running a second dbt build."
            )
            self.log.error(retry_message)
            raise AirflowException(retry_message)

        # This global variable is used to make the task context available to the K8s callback.
        # While the callback is set during the operator initialization, the context is only created during the operator's execution.
        global producer_task_context
        producer_task_context = context
        return super().execute(context, **kwargs)


class DbtConsumerWatcherKubernetesSensor(BaseConsumerSensor, DbtRunKubernetesOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]

    def _use_event(self) -> bool:
        return False


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the producer task.
# That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD, until we have a better experience.
class DbtBuildWatcherKubernetesOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherKubernetesOperator(DbtSeedMixin, DbtConsumerWatcherKubernetesSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherKubernetesOperator(DbtSnapshotMixin, DbtConsumerWatcherKubernetesSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields


class DbtSourceWatcherKubernetesOperator(DbtSourceKubernetesOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.LOCAL.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceKubernetesOperator.template_fields)  # type: ignore[arg-type]


class DbtRunWatcherKubernetesOperator(DbtConsumerWatcherKubernetesSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtTestWatcherKubernetesOperator(EmptyOperator):
    """
    As a starting point, this operator does nothing.
    We'll be implementing this operator as part of: https://github.com/astronomer/astronomer-cosmos/issues/1974
    """

    def __init__(self, *args: Any, **kwargs: Any):
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
