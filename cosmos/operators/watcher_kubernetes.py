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

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
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


class WatcherKubernetesCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """Callback that parses dbt JSON logs from Kubernetes pod output.

    State (context, test maps) is bound to each instance by the producer.
    """

    def __init__(
        self,
        tests_per_model: dict[str, list[str]],
        test_results_per_model: dict[str, list[str]],
    ) -> None:
        self.tests_per_model = tests_per_model
        self.test_results_per_model = test_results_per_model
        self.context: Context | None = None

    def progress_callback(
        self,
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
            kwargs["context"] = self.context
        store_dbt_resource_status_from_log(
            line,
            kwargs,
            tests_per_model=self.tests_per_model,
            test_results_per_model=self.test_results_per_model,
        )


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):
    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")
        tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self._watcher_callback = WatcherKubernetesCallback(tests_per_model, test_results_per_model={})

        existing_callbacks = kwargs.get("callbacks")
        if existing_callbacks is None:
            normalized_callbacks: list[Any] = []
        elif isinstance(existing_callbacks, (list, tuple)):
            normalized_callbacks = list(existing_callbacks)
        else:
            normalized_callbacks = [existing_callbacks]
        normalized_callbacks.append(self._watcher_callback)
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

        self._watcher_callback.context = context
        return super().execute(context, **kwargs)


class DbtConsumerWatcherKubernetesSensor(BaseConsumerSensor, DbtRunKubernetesOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]


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


class DbtTestWatcherKubernetesOperator(DbtConsumerWatcherKubernetesSensor):
    """Sensor that watches the aggregated test status for a dbt model in WATCHER_KUBERNETES execution mode.

    The producer task collects individual test results as they finish and,
    once every test for a given model has reported, pushes a single
    aggregated XCom (``"pass"`` or ``"fail"``) under the key returned by
    ``get_tests_status_xcom_key(model_uid)``. This key sanitizes the dbt
    model unique ID by replacing ``.`` with ``__`` before appending
    ``_tests_status`` (for example,
    ``model.jaffle_shop.stg_orders`` becomes
    ``model__jaffle_shop__stg_orders_tests_status``).

    This sensor polls that key and returns success when ``"pass"`` or raises
    ``AirflowException`` when ``"fail"``.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields

    @property
    def is_test_sensor(self) -> bool:
        return True
