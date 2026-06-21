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
from cosmos.dbt.graph import DbtNode
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor
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


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):
    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self._tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self._test_results_per_model: dict[str, dict[str, str]] = {}
        # Set in execute() before super().execute() triggers pod_manager. Initialized
        # here so pod_manager never raises AttributeError if accessed before execute().
        self._context: Context | None = None
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


class DbtConsumerWatcherKubernetesSensor(BaseConsumerSensor, DbtRunKubernetesOperator):
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the producer task.
# That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD, until we have a better experience.
class DbtBuildWatcherKubernetesOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER_KUBERNETES` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherKubernetesOperator(DbtSeedMixin, DbtConsumerWatcherKubernetesSensor):
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherKubernetesOperator(DbtSnapshotMixin, DbtConsumerWatcherKubernetesSensor):
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields


class DbtSourceWatcherKubernetesOperator(DbtSourceKubernetesOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.KUBERNETES.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceKubernetesOperator.template_fields)


class DbtRunWatcherKubernetesOperator(DbtConsumerWatcherKubernetesSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
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

    On manual clear from the Airflow UI or Airflow-level retry, the sensor falls
    back to launching a pod that runs ``dbt test --select <model>`` for this
    specific model.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields

    # See DbtTestWatcherOperator for the reasoning behind hardcoding base_cmd
    # rather than inheriting from DbtTestMixin.
    base_cmd = ["test"]

    @property
    def is_test_sensor(self) -> bool:
        return True

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """Launch a pod running ``dbt test --select <model>`` to retry tests for this model.

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
