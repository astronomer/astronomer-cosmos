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
from cosmos.constants import PRODUCER_WATCHER_TASK_ID
from cosmos.dataset import register_dataset_on_task
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
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self._tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self._test_results_per_model: dict[str, dict[str, str]] = {}
        # Whether the producer should compute per-model outlet URIs. The producer never emits
        # datasets itself (the consumer sensors do), but it must build the URI map so each
        # consumer can. Wired as an explicit flag by _add_watcher_producer_task.
        self._should_generate_model_uris: bool = kwargs.pop(
            "_should_generate_model_uris", kwargs.get("emit_datasets", True)
        )
        # manifest_filepath is threaded through task_args by cosmos.converter (from
        # ProjectConfig.manifest_path). The pod's own target/manifest.json lives inside the
        # container and is not reachable from the scheduler, so this scheduler-side manifest is
        # the only practical source for building the outlet URI map. Popped here because the
        # K8s base operator (unlike the local one) doesn't accept it.
        self.manifest_filepath: str = kwargs.pop("manifest_filepath", "") or ""
        # Mutable per-execution state shared by reference with the log-parsing callback via the
        # pod manager's callback_extra_kwargs. execute() resolves the namespace and fills the URI
        # map in place (the dict is never reassigned), so a pod_manager created earlier still
        # observes the populated map.
        self._dataset_namespace: str | None = None
        self._model_outlet_uris: dict[str, list[str]] = {}
        # Mutable holder shared by reference with pod_manager's callback_extra_kwargs.
        # execute() sets its "context" entry (the holder itself is never reassigned),
        # so a pod_manager created before execute() still sees the live context.
        self._context_holder: dict[str, Context | None] = {_k8s_common.CONTEXT_KEY: None}
        _k8s_common.inject_watcher_callback(kwargs)
        super().__init__(task_id=task_id, *args, **kwargs)
        self.dbt_cmd_flags += ["--log-format", "json"]
        # Flush the in-memory XCom backup to a Variable on failure so the producer retry can restore it.
        _k8s_common.compose_watcher_backup_callbacks(self)
        # Mutable set populated by the log parser when dbt emits SkippingDetails
        # or LogSkipBecauseError for a node; subsequent "skipped" terminal events
        # for those unique_ids are rewritten to "failed" so the consumer sensor
        # fails on attempt 1 (instead of SKIPPED, which Airflow will not retry).
        # Mirrors DbtProducerWatcherOperator._upstream_failure_skipped_ids; see #2698.
        self._upstream_failure_skipped_ids: set[str] = set()

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return _k8s_common.build_watcher_pod_manager(self)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # Bind before passing, because bare super() doesn't work inside lambdas or when called outside this method.
        parent_execute = super().execute
        return _k8s_common.execute_watcher_producer(self, context, parent_execute, **kwargs)


class DbtConsumerWatcherGcpGkeSensor(BaseConsumerSensor, DbtRunGcpGkeOperator):
    """Consumer sensor for ``ExecutionMode.WATCHER_GCP_GKE``.

    Polls the producer's per-node status XCom and, on successful model completion,
    emits one Airflow Asset per outlet URI the producer computed from the manifest --
    making dbt model lineage visible to downstream catalogs that read Airflow outlets
    (OpenLineage, OpenMetadata, etc.). Mirrors ``DbtConsumerWatcherKubernetesSensor``,
    delegating to ``register_dataset_on_task`` because this sensor doesn't inherit from
    the local operator.
    """

    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunGcpGkeOperator.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def _emit_datasets(self, context: Context) -> None:
        """Emit one Airflow ``Asset`` per outlet URI received from the producer.

        No-ops when ``self.emit_datasets`` is False (user disabled emission) or when
        ``self._outlet_uris`` is empty (the producer resolved no outlets for this model --
        typical when no manifest was available or the adapter has no OL namespace resolver).
        """
        if not getattr(self, "emit_datasets", False):
            return
        outlet_uris = getattr(self, "_outlet_uris", [])
        if not outlet_uris:
            return

        from cosmos import settings
        from cosmos.constants import AIRFLOW_VERSION

        if AIRFLOW_VERSION.major >= 3:
            from airflow.sdk.definitions.asset import Asset
        else:
            from airflow.datasets import Dataset as Asset  # type: ignore[no-redef]

        outlets = [Asset(uri=uri) for uri in outlet_uris]
        logger.info("Emitting %d dataset(s) for model '%s': %s", len(outlets), self.model_unique_id, outlet_uris)
        register_dataset_on_task(self, [], outlets, context)

        if settings.enable_uri_xcom:
            context["ti"].xcom_push(key="uri", value=outlet_uris)

    def execute(self, context: Context, **kwargs: Any) -> None:
        super().execute(context, **kwargs)
        # If we reach here without deferring, the model succeeded -- emit datasets.
        self._emit_datasets(context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        # Extract outlet URIs from the trigger event before the parent handles status.
        self._outlet_uris = event.get("outlet_uris", [])
        super().execute_complete(context, event)
        # If we reach here without raising, the model succeeded -- emit datasets.
        self._emit_datasets(context)


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
