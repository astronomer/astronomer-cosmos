from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

import cosmos.operators._k8s_common as _k8s_common
from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators.base import (
    DbtRunMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
)
from cosmos.operators.gcp_gke import (
    DbtBuildGcpGkeOperator,
    DbtRunGcpGkeOperator,
    DbtSourceGcpGkeOperator,
)


class DbtProducerWatcherGcpGkeOperator(DbtBuildGcpGkeOperator):
    template_fields: tuple[str, ...] = tuple(DbtBuildGcpGkeOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")
        _k8s_common.init_watcher_producer(_k8s_common.WatcherK8sCallback, kwargs)
        super().__init__(task_id=task_id, *args, **kwargs)
        self.dbt_cmd_flags += ["--log-format", "json"]

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return CosmosKubernetesPodManager(kube_client=self.client, callbacks=self.callbacks)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # Bind before passing, because bare super() doesn't work inside lambdas or when called outside this method.
        parent_execute = super().execute
        return _k8s_common.execute_watcher_producer(self, context, parent_execute, **kwargs)


class DbtConsumerWatcherGcpGkeSensor(BaseConsumerSensor, DbtRunGcpGkeOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunGcpGkeOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the
# producer task. That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD,
# until we have a better experience.
class DbtBuildWatcherGcpGkeOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER_GCP_GKE` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )


class DbtSeedWatcherGcpGkeOperator(DbtSeedMixin, DbtConsumerWatcherGcpGkeSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherGcpGkeOperator(DbtSnapshotMixin, DbtConsumerWatcherGcpGkeSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceWatcherGcpGkeOperator(DbtSourceGcpGkeOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.GCP_GKE.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceGcpGkeOperator.template_fields)  # type: ignore[arg-type]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunWatcherGcpGkeOperator(DbtConsumerWatcherGcpGkeSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWatcherGcpGkeOperator(EmptyOperator):
    """
    As a starting point, this operator does nothing.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
