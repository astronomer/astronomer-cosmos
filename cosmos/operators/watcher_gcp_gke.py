from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

import cosmos.operators._k8s_common as _k8s_common
from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.dbt.graph import DbtNode
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor
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

logger = get_logger(__name__)


class DbtProducerWatcherGcpGkeOperator(DbtBuildGcpGkeOperator):
    template_fields: tuple[str, ...] = tuple(DbtBuildGcpGkeOperator.template_fields) + ("deferrable",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = _k8s_common.init_watcher_producer(self, kwargs)
        super().__init__(task_id=task_id, *args, **kwargs)
        _k8s_common.finalize_watcher_producer(self)

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return _k8s_common.build_watcher_pod_manager(self)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # Bind before passing, because bare super() doesn't work inside lambdas or when called outside this method.
        parent_execute = super().execute
        return _k8s_common.execute_watcher_producer(self, context, parent_execute, **kwargs)


class DbtConsumerWatcherGcpGkeSensor(BaseConsumerSensor, DbtRunGcpGkeOperator):
    """Consumer sensor for ``ExecutionMode.WATCHER_GCP_GKE``.

    Polls the producer's per-node status XCom and, on successful model completion, emits one
    Airflow Asset per outlet URI the producer computed from the manifest -- making dbt model
    lineage visible to downstream catalogs that read Airflow outlets (OpenLineage, OpenMetadata,
    etc.). Dataset emission (``_emit_datasets`` plus the ``execute`` / ``execute_complete`` hooks)
    is inherited from ``BaseConsumerSensor``.
    """

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


class DbtSeedWatcherGcpGkeOperator(DbtSeedMixin, DbtConsumerWatcherGcpGkeSensor):
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherGcpGkeOperator(DbtSnapshotMixin, DbtConsumerWatcherGcpGkeSensor):
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

    template_fields: tuple[str, ...] = tuple(DbtSourceGcpGkeOperator.template_fields)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunWatcherGcpGkeOperator(DbtConsumerWatcherGcpGkeSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWatcherGcpGkeOperator(DbtConsumerWatcherGcpGkeSensor):
    """Sensor that watches the aggregated test status for a dbt model in WATCHER_GCP_GKE execution mode.

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

    On manual clear from the Airflow UI or Airflow-level retry, the sensor falls
    back to launching a GKE pod that runs ``dbt test --select <model>`` for this
    specific model.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherGcpGkeSensor.template_fields

    # See DbtTestWatcherOperator for the reasoning behind hardcoding base_cmd
    # rather than inheriting from DbtTestMixin.
    base_cmd = ["test"]

    @property
    def is_test_sensor(self) -> bool:
        return True

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """Launch a GKE pod running ``dbt test --select <model>`` to retry tests for this model.

        Used when the test sensor is manually cleared from the Airflow UI or when
        Airflow-level retries fire. Producer flags are intentionally not forwarded
        because some of them (e.g. ``--full-refresh``) are not valid for ``dbt test``.
        """
        logger.info(
            "Running tests for model '%s' from project '%s' (try %s)",
            self.model_unique_id,
            self.project_dir,
            try_number,
        )

        model_selector = DbtNode.get_resource_name_from_unique_id(self.model_unique_id)
        cmd_flags = ["--select", model_selector]
        self.build_and_run_cmd(context, cmd_flags=cmd_flags)
        logger.info("dbt test completed successfully for model '%s'", self.model_unique_id)
        return True
