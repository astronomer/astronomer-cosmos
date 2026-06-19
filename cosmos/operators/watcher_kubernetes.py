from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pendulum import DateTime

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

import kubernetes.client as k8s
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback, client_type

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.constants import PRODUCER_WATCHER_TASK_ID
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators._watcher.xcom import (
    _compose_backup_callback,
    _delete_xcom_backup_variable,
    _init_xcom_backup,
    _restore_xcom_from_variable,
)
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

    ``tests_per_model``, ``test_results_per_model``, ``context``, and
    ``upstream_failure_skipped_ids`` are forwarded by ``CosmosKubernetesPodManager``
    via its ``callback_extra_kwargs`` and arrive in ``progress_callback`` as ``**kwargs``.
    The ``receives_cosmos_callback_kwargs`` marker opts this callback in to receiving
    them; user-supplied callbacks without the marker are not given these Cosmos-only
    kwargs, so their ``progress_callback`` is not broken.
    """

    # Opts this callback in to receiving callback_extra_kwargs; read by
    # CosmosKubernetesPodManager._extra_kwargs_for (keep the attribute name in sync).
    receives_cosmos_callback_kwargs = True

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
        store_dbt_resource_status_from_log(
            line,
            kwargs,
            tests_per_model=kwargs.get("tests_per_model"),
            test_results_per_model=kwargs.get("test_results_per_model"),
            upstream_failure_skipped_ids=kwargs.get("upstream_failure_skipped_ids"),
        )


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):
    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self._tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self._test_results_per_model: dict[str, dict[str, str]] = {}
        # Set in execute() before super().execute() triggers pod_manager. Initialized
        # here so pod_manager never raises AttributeError if accessed before execute().
        self._context: Context | None = None

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
        # Flush the in-memory backup to a Variable so the producer retry can restore it. A graceful
        # failure with retries left is UP_FOR_RETRY (on_retry_callback), not FAILED, so register on
        # both; composed after super().__init__ to preserve a DAG-level default_args callback (#2776).
        self.on_retry_callback = _compose_backup_callback(getattr(self, "on_retry_callback", None))
        self.on_failure_callback = _compose_backup_callback(getattr(self, "on_failure_callback", None))
        # Mutable set populated by the log parser when dbt emits SkippingDetails
        # or LogSkipBecauseError for a node; subsequent "skipped" terminal events
        # for those unique_ids are rewritten to "failed" so the consumer sensor
        # fails on attempt 1 (instead of SKIPPED, which Airflow will not retry).
        # Mirrors DbtProducerWatcherOperator._upstream_failure_skipped_ids; see #2698.
        self._upstream_failure_skipped_ids: set[str] = set()

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return CosmosKubernetesPodManager(
            kube_client=self.client,
            callbacks=self.callbacks,
            callback_extra_kwargs={
                "tests_per_model": self._tests_per_model,
                "test_results_per_model": self._test_results_per_model,
                "context": self._context,
                "upstream_failure_skipped_ids": self._upstream_failure_skipped_ids,
            },
        )

    def execute(self, context: Context, **kwargs: Any) -> Any:
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException(
                "DbtProducerWatcherKubernetesOperator expects a task instance in the execution context"
            )

        try_number = getattr(task_instance, "try_number", 1)

        from cosmos import settings

        reliable_retry = settings.enable_watcher_reliable_retry

        if try_number > 1:
            _restore_xcom_from_variable(context)
            raise AirflowSkipException(
                "DbtProducerWatcherKubernetesOperator does not support Airflow retries. "
                f"Detected attempt #{try_number}; skipping execution to avoid running a second dbt build."
            )

        _init_xcom_backup(context, persist=reliable_retry)

        self._upstream_failure_skipped_ids.clear()
        self._context = context

        # On failure super().execute() raises and the on-failure callback flushes the backup.
        return_value = super().execute(context, **kwargs)
        if reliable_retry:
            _delete_xcom_backup_variable(context)
        return return_value


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

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherKubernetesOperator(DbtSnapshotMixin, DbtConsumerWatcherKubernetesSensor):
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherKubernetesSensor.template_fields


class DbtSourceWatcherKubernetesOperator(DbtSourceKubernetesOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.LOCAL.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceKubernetesOperator.template_fields)


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

        model_selector = self.model_unique_id.split(".", 2)[2]
        cmd_flags = ["--select", model_selector]
        self.build_and_run_cmd(context, cmd_flags=cmd_flags)
        logger.info("dbt test completed successfully for model '%s'", self.model_unique_id)
        return True
