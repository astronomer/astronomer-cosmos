from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from pathlib import Path
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

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.dataset import compute_model_outlet_uris, get_dataset_namespace, register_dataset_on_task
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators._watcher.xcom import (
    _backup_xcom_to_variable,
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


# Module-level globals used to bridge producer-operator state into the
# `WatcherKubernetesCallback.progress_callback` static method. The callback
# is registered on the KubernetesPodOperator at init time, well before the
# operator's execute() runs — so producer state has to be published through
# the module namespace rather than closed over.
producer_task_context = None
producer_task_model_outlet_uris: dict[str, list[str]] | None = None
producer_task_dataset_namespace: str | None = None


class WatcherKubernetesCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]
    """
    KubernetesPodOperator callback that forwards each pod log line to
    ``store_dbt_resource_status_from_log``.

    Mirrors the SUBPROCESS watcher's log-parsing callback, including the
    ``model_outlet_uris`` and ``dataset_namespace`` bindings that let
    consumer sensors emit per-model Airflow datasets. The bindings are
    read from the module-level ``producer_task_*`` globals populated by
    the producer operator's ``execute`` — see the module docstring above
    for why the globals are needed (static callback, set before execute).
    """

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
        Process a single log line emitted by the watcher pod.

        :param line: the read line of log.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        :param container_name: the name of the container from which the log line was read.
        :param timestamp: the timestamp of the log line.
        :param pod: the pod from which the log line was read.
        """
        if "context" not in kwargs:
            kwargs["context"] = producer_task_context
        store_dbt_resource_status_from_log(
            line,
            kwargs,
            model_outlet_uris=producer_task_model_outlet_uris,
            dataset_namespace=producer_task_dataset_namespace,
        )


class DbtProducerWatcherKubernetesOperator(DbtBuildKubernetesOperator):
    """
    Producer operator for ``ExecutionMode.WATCHER_KUBERNETES``.

    Runs ``dbt build`` inside a Kubernetes pod, streams JSON log events back
    to the scheduler, and updates per-node XCom status for downstream
    consumer sensors. Each consumer reads its model's ``outlet_uris`` from
    the XCom payload and emits Airflow Assets so lineage-aware catalogs
    (OpenLineage, OpenMetadata, etc.) can draw task ↔ dataset edges.

    The outlet URI map is built on the scheduler at ``execute`` time by
    reading ``ProjectConfig.manifest_path`` (threaded through as
    ``manifest_filepath``). The pod's own ``target/manifest.json`` lives
    inside the container and is not reachable from the scheduler, so this
    is the only practical source.
    """

    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")
        # manifest_filepath is threaded through task_args by cosmos.converter
        # (from ProjectConfig.manifest_path). Stored on self so execute() can
        # read the manifest on the scheduler to build the outlet URI map.
        self.manifest_filepath: str = kwargs.pop("manifest_filepath", "") or ""

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

        # Populated in execute() from the manifest; shared with the log-parsing
        # callback via module-level globals (see module docstring).
        self._model_outlet_uris: dict[str, list[str]] = {}
        self._dataset_namespace: str | None = None

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return CosmosKubernetesPodManager(kube_client=self.client, callbacks=self.callbacks)

    def _populate_model_outlet_uris(self) -> None:
        """
        Populate ``self._model_outlet_uris`` from the scheduler-side manifest.

        Called once at the start of ``execute``. Silently skips if no
        dataset namespace could be resolved (e.g. an adapter without an
        OpenLineage-compatible namespace resolver) or if the manifest file
        isn't available. Matches the SUBPROCESS watcher's degraded-mode
        behaviour: dataset emission becomes a no-op, but dbt execution
        and status reporting still work.
        """
        if not self._dataset_namespace:
            return
        if not self.manifest_filepath:
            logger.warning(
                "manifest_filepath not supplied to %s; dataset emission will be disabled for this run. "
                "Pass ProjectConfig.manifest_path to enable per-model outlets.",
                type(self).__name__,
            )
            return
        manifest_path = Path(self.manifest_filepath)
        if not manifest_path.exists():
            logger.warning(
                "Manifest not found at %s; dataset emission will be disabled for this run.",
                manifest_path,
            )
            return
        self._model_outlet_uris.update(compute_model_outlet_uris(manifest_path, self._dataset_namespace))

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # get_dataset_namespace requires a ProfileConfig; skip outlet resolution
        # when one wasn't supplied (e.g. some test constructions, or DAGs that
        # inline dbt profiles via environment / profiles_yml_filepath only
        # without wrapping them in ProfileConfig). Dataset emission then
        # degrades to a no-op, matching the missing-manifest behaviour.
        if self.profile_config is not None:
            self._dataset_namespace = get_dataset_namespace(self.profile_config)
        else:
            self._dataset_namespace = None
        self._model_outlet_uris.clear()
        self._populate_model_outlet_uris()

        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException(
                "DbtProducerWatcherKubernetesOperator expects a task instance in the execution context"
            )

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            _restore_xcom_from_variable(context)
            raise AirflowSkipException(
                "DbtProducerWatcherKubernetesOperator does not support Airflow retries. "
                f"Detected attempt #{try_number}; skipping execution to avoid running a second dbt build."
            )

        _init_xcom_backup(context)

        # Publish producer state into module globals so the static
        # WatcherKubernetesCallback.progress_callback can read it during log
        # processing. Must happen before super().execute() dispatches the pod.
        global producer_task_context, producer_task_model_outlet_uris, producer_task_dataset_namespace
        producer_task_context = context
        producer_task_model_outlet_uris = self._model_outlet_uris
        producer_task_dataset_namespace = self._dataset_namespace

        try:
            return_value = super().execute(context, **kwargs)
            _delete_xcom_backup_variable(context)
            return return_value
        except Exception:
            _backup_xcom_to_variable(context)
            raise


class DbtConsumerWatcherKubernetesSensor(BaseConsumerSensor, DbtRunKubernetesOperator):
    """
    Consumer sensor for ``ExecutionMode.WATCHER_KUBERNETES``.

    Polls the producer's per-node status XCom and, on successful model
    completion, emits one Airflow Asset per outlet URI the producer
    computed from the manifest. This is what makes dbt model lineage
    visible to downstream catalogs that read Airflow outlets.

    Mirrors ``DbtConsumerWatcherSensor`` (the SUBPROCESS equivalent).
    """

    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunKubernetesOperator.template_fields  # type: ignore[operator]

    def _emit_datasets(self, context: Context) -> None:
        """
        Emit one Airflow ``Asset`` per outlet URI received from the producer.

        No-ops when either:

        - ``self.emit_datasets`` is False (user disabled dataset emission), or
        - ``self._outlet_uris`` is empty (producer resolved no outlets for
          this model — typical when no manifest was available or the model's
          adapter has no OL namespace resolver).
        """
        if not getattr(self, "emit_datasets", False):
            return
        outlet_uris = getattr(self, "_outlet_uris", [])
        if not outlet_uris:
            return

        from cosmos.constants import AIRFLOW_VERSION

        if AIRFLOW_VERSION.major >= 3:
            from airflow.sdk.definitions.asset import Asset
        else:
            from airflow.datasets import Dataset as Asset  # type: ignore[no-redef]

        outlets = [Asset(uri=uri) for uri in outlet_uris]
        logger.info(
            "Emitting %d dataset(s) for model '%s': %s",
            len(outlets),
            getattr(self, "model_unique_id", "<unknown>"),
            outlet_uris,
        )
        register_dataset_on_task(self, [], outlets, context)

    def execute(self, context: Context, **kwargs: Any) -> None:  # type: ignore[override]
        """Run the synchronous sensor loop, then emit datasets on success."""
        super().execute(context, **kwargs)
        self._emit_datasets(context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Resume from a trigger event.

        Extracts ``outlet_uris`` from the event payload so ``_emit_datasets``
        (called after the parent's status handling) has something to emit.
        """
        self._outlet_uris = event.get("outlet_uris", [])
        super().execute_complete(context, event)
        self._emit_datasets(context)


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
