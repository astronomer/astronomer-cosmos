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

from airflow.exceptions import AirflowException

try:
    from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback, client_type
except Exception:  # pragma: no cover
    KubernetesPodOperatorCallback = object  # type: ignore[assignment,misc]
    client_type = object  # type: ignore[assignment]

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators.base import DbtRunMixin, DbtSeedMixin, DbtSnapshotMixin
from cosmos.operators.gke import DbtBuildGkeOperator, DbtRunGkeOperator, DbtSourceGkeOperator

logger = get_logger(__name__)

# Same pattern as watcher_kubernetes: make runtime context available to callback.
producer_task_context: Context | None = None


class WatcherGkeCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    @staticmethod
    def progress_callback(
        *,
        line: str,
        client: client_type,
        mode: str,
        container_name: str,
        timestamp: DateTime | None,
        pod: Any,
        **kwargs: Any,
    ) -> None:
        logger.info("WatcherGkeCallback invoked (container=%s)", container_name)
        if "context" not in kwargs:
            kwargs["context"] = producer_task_context
        store_dbt_resource_status_from_log(line, kwargs)


class DbtProducerWatcherGkeOperator(DbtBuildGkeOperator):
    """
    Producer task for ExecutionMode.WATCHER_GKE.

    Runs `dbt build --log-format json` on GKE and streams JSON log lines
    into XCom (via the callback -> store_dbt_resource_status_from_log).
    """

    template_fields: tuple[str, ...] = tuple(DbtBuildGkeOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")

        # Disable retries on producer task
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0

        # Attach callback (GKEStartPodOperator may or may not expose callbacks depending on provider version).
        existing_callbacks = kwargs.get("callbacks")
        if existing_callbacks is None:
            normalized_callbacks: list[Any] = []
        elif isinstance(existing_callbacks, (list, tuple)):
            normalized_callbacks = list(existing_callbacks)
        else:
            normalized_callbacks = [existing_callbacks]
        normalized_callbacks.append(WatcherGkeCallback)
        kwargs["callbacks"] = normalized_callbacks

        super().__init__(task_id=task_id, *args, **kwargs)

        # 🔧 ensure callbacks are available even if the base operator doesn't set self.callbacks
        self.callbacks = normalized_callbacks  # type: ignore[attr-defined]
        # watcher mode needs dbt json logs
        self.dbt_cmd_flags += ["--log-format", "json"]

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        callbacks = getattr(self, "callbacks", [])
        return CosmosKubernetesPodManager(kube_client=self.client, callbacks=callbacks)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        ti = context.get("ti")
        if ti is None:
            raise AirflowException("DbtProducerWatcherGkeOperator expects a task instance in the execution context")

        try_number = getattr(ti, "try_number", 1)
        if try_number > 1:
            self.log.info(
                "DbtProducerWatcherGkeOperator does not support Airflow retries. "
                "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
                try_number,
            )
            return None

        global producer_task_context
        producer_task_context = context
        return super().execute(context, **kwargs)


class DbtConsumerWatcherGkeSensor(BaseConsumerSensor, DbtRunGkeOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + tuple(DbtRunGkeOperator.template_fields)  # type: ignore[operator]

    def use_event(self) -> bool:
        return False


class DbtBuildWatcherGkeOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER_*` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherGkeOperator(DbtSeedMixin, DbtConsumerWatcherGkeSensor):  # type: ignore[misc]
    template_fields: tuple[str, ...] = DbtConsumerWatcherGkeSensor.template_fields + tuple(DbtSeedMixin.template_fields)  # type: ignore[operator]


class DbtSnapshotWatcherGkeOperator(DbtSnapshotMixin, DbtConsumerWatcherGkeSensor):  # type: ignore[misc]
    template_fields: tuple[str, ...] = DbtConsumerWatcherGkeSensor.template_fields


class DbtSourceWatcherGkeOperator(DbtSourceGkeOperator):
    # Mirrors watcher_kubernetes behavior: source freshness is run directly (not watched)
    template_fields: tuple[str, ...] = tuple(DbtSourceGkeOperator.template_fields)  # type: ignore[arg-type]


class DbtRunWatcherGkeOperator(DbtConsumerWatcherGkeSensor):
    template_fields: tuple[str, ...] = DbtConsumerWatcherGkeSensor.template_fields + tuple(DbtRunMixin.template_fields)  # type: ignore[operator]


class DbtTestWatcherGkeOperator(EmptyOperator):
    def __init__(self, *args: Any, **kwargs: Any):
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
