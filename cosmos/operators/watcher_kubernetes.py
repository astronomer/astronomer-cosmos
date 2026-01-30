from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any
import re

if TYPE_CHECKING:  # pragma: no cover
    from pendulum import DateTime

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

import kubernetes.client as k8s
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
from airflow.models.taskinstance import TaskInstance

from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode
import asyncio
from functools import wraps

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log, store_dbt_resource_status_to_xcom
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


def serializable_callback(f):
    """Convert async callback so it can run in sync or async mode."""

    @wraps(f)
    def wrapper(*args, mode: str, **kwargs):
        if mode == ExecutionMode.ASYNC:
            return f(*args, mode=mode, **kwargs)
        return asyncio.run(f(*args, mode=mode, **kwargs))

    return wrapper


def get_task_instance_from_pod(pod: k8s.V1Pod) -> TaskInstance:
    run_id = pod.metadata.labels["run_id"]
    m_run_id = re.match(r"^(?P<prefix>.*?__\d{4}-\d{2}-\d{2}T)(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})(?P<f>\d*)(?P<oH>-?\d{2})(?P<oM>\d{2})-", run_id)
    p_run_id = m_run_id.groupdict()
    if p_run_id["f"]:
        p_run_id["f"] = f".{p_run_id['f']}"
    if not p_run_id["oH"].startswith("-"):
        p_run_id["oH"] = f"+{p_run_id['oH']}"
    fixed_run_id = (
        f"{p_run_id['prefix']}{p_run_id['H']}:{p_run_id['M']}:"
        f"{p_run_id['S']}{p_run_id['f']}{p_run_id['oH']}:{p_run_id['oM']}"
    )
    return TaskInstance.get_task_instance(
        dag_id=pod.metadata.labels["dag_id"],
        task_id=pod.metadata.labels["task_id"],
        run_id=fixed_run_id,
        map_index=int(pod.metadata.labels.get("map_index", -1)),
    )

class WatcherKubernetesCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    task_instance: TaskInstance | None = None

    @classmethod
    @serializable_callback
    async def progress_callback(
        cls,
        *,
        line: str,
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
        if cls.task_instance is None:
            cls.task_instance = get_task_instance_from_pod(pod)
        store_dbt_resource_status_to_xcom(line, cls.task_instance)


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):

    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")

        # Disable retries on producer task
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0

        existing_callbacks = kwargs.get("callbacks")
        if existing_callbacks is None:
            normalized_callbacks: list[Any] = []
        elif isinstance(existing_callbacks, (list, tuple)):
            normalized_callbacks = list(existing_callbacks)
        else:
            normalized_callbacks = [existing_callbacks]
        normalized_callbacks.append(WatcherKubernetesCallback)
        kwargs["callbacks"] = normalized_callbacks
        super().__init__(task_id=task_id, *args, **kwargs)
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
            self.log.info(
                "DbtProducerWatcherKubernetesOperator does not support Airflow retries. "
                "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
                try_number,
            )
            return None

        return super().execute(context, **kwargs)


class DbtConsumerWatcherKubernetesSensor(BaseConsumerSensor, DbtRunKubernetesOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]

    def use_event(self) -> bool:
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
