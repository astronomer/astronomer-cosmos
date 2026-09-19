"""Container-agnostic helpers shared by the watcher producer operators.

The watcher producer runs one ``dbt build`` and publishes per-node statuses parsed from dbt's JSON
log lines. How those lines are obtained depends on where the container runs (pod log stream, log
service polling, ...) and lives with each flavour; the state the parser needs, the retry semantics
and the XCom backup handling do not, and live here so a flavour can be added without importing
another flavour's client libraries.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

from cosmos.airflow.compatibility import AirflowSkipException
from cosmos.constants import _PRODUCER_CMD_FLAGS_XCOM_KEY, PRODUCER_WATCHER_TASK_ID
from cosmos.operators._watcher import safe_xcom_push
from cosmos.operators._watcher.xcom import (
    _compose_backup_callback,
    _delete_xcom_backup_variable,
    _init_xcom_backup,
    _restore_xcom_from_variable,
)

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]


# Contract keys between operator and callback.
CONTEXT_HOLDER_KEY = "context_holder"
CONTEXT_KEY = "context"


# The container-based watcher producers (``DbtProducerWatcherKubernetesOperator``,
# ``DbtProducerWatcherGcpGkeOperator``) share their per-execution state and the helpers below that
# read/write it. ``init_watcher_producer`` seeds that state; the operator carries the following
# contract (typed ``Any`` in the helpers, mirroring ``compose_watcher_backup_callbacks``, because
# the state is populated dynamically rather than declared on the concrete operator classes):
#   _tests_per_model, _test_results_per_model, _context_holder, _upstream_failure_skipped_ids,
#   _should_generate_model_uris, _dataset_namespace, _model_outlet_uris, manifest_filepath,
#   dbt_cmd_flags, profile_config, log.


def init_watcher_producer(operator: Any, kwargs: dict[str, Any]) -> str:
    """Shared pre-``super().__init__`` setup for container-based watcher producers.

    Seeds the per-execution state that ``execute_watcher_producer`` and the log-parsing hook of each
    container flavour rely on, and returns the resolved ``task_id`` for the caller to forward to
    ``super().__init__``. The Kubernetes flavours add their pod-log callback on top
    (``cosmos.operators._k8s_common.init_watcher_producer``).

    :param operator: the producer instance being initialised.
    :param kwargs: the operator's ``__init__`` kwargs; watcher-only keys are popped in place.
    :returns: the resolved ``task_id``.
    """
    task_id: str = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
    operator._tests_per_model = kwargs.pop("tests_per_model", {})
    operator._test_results_per_model = {}
    # Whether the producer should compute per-model outlet URIs. The producer never emits datasets
    # itself (the consumer sensors do), but it must build the URI map so each consumer can. Wired as
    # an explicit flag by _add_watcher_producer_task.
    operator._should_generate_model_uris = kwargs.pop("_should_generate_model_uris", kwargs.get("emit_datasets", True))
    # manifest_filepath is threaded through task_args by cosmos.converter (from
    # ProjectConfig.manifest_path). The pod's own target/manifest.json lives inside the container and
    # is not reachable from the scheduler, so this scheduler-side manifest is the only practical
    # source for the outlet URI map. Popped here because the K8s base operator (unlike the local one)
    # doesn't accept it.
    operator.manifest_filepath = kwargs.pop("manifest_filepath", "") or ""
    # Mutable per-execution state shared by reference with the log-parsing callback via the pod
    # manager's callback_extra_kwargs. execute() resolves the namespace and fills the URI map in
    # place (never reassigns), so a pod_manager created earlier still observes the populated map.
    operator._dataset_namespace = None
    operator._model_outlet_uris = {}
    # Mutable holder shared by reference with pod_manager's callback_extra_kwargs. execute() sets its
    # "context" entry (the holder itself is never reassigned), so a pod_manager created before
    # execute() still sees the live context.
    operator._context_holder = {CONTEXT_KEY: None}
    return task_id


def finalize_watcher_producer(operator: Any) -> None:
    """Shared post-``super().__init__`` setup for container-based watcher producers.

    Forces the dbt JSON log format the parser depends on, wires the XCom-backup flush onto the
    producer's retry/failure callbacks, and seeds the upstream-failure tracking set. Must run after
    ``super().__init__`` so ``compose_watcher_backup_callbacks`` can preserve a DAG-level
    ``default_args`` callback (#2776).
    """
    operator.dbt_cmd_flags += ["--log-format", "json"]
    compose_watcher_backup_callbacks(operator)
    # Populated by the log parser when dbt emits SkippingDetails or LogSkipBecauseError for a node;
    # subsequent "skipped" terminal events for those unique_ids are rewritten to "failed" so the
    # consumer sensor fails on attempt 1 (instead of SKIPPED, which Airflow will not retry).
    # Mirrors DbtProducerWatcherOperator._upstream_failure_skipped_ids; see #2698.
    operator._upstream_failure_skipped_ids = set()


def compose_watcher_backup_callbacks(operator: Any) -> None:
    """Append the XCom backup flush to the producer's retry/failure callbacks.

    A graceful failure with retries left is UP_FOR_RETRY (on_retry_callback), not
    FAILED, so register on both. Must be called after ``super().__init__`` to preserve
    a DAG-level ``default_args`` callback (#2776).
    """
    operator.on_retry_callback = _compose_backup_callback(getattr(operator, "on_retry_callback", None))
    operator.on_failure_callback = _compose_backup_callback(getattr(operator, "on_failure_callback", None))


def _populate_producer_model_outlet_uris(operator: Any) -> None:
    """Resolve the dataset namespace and fill ``operator._model_outlet_uris`` from the manifest.

    Mirrors the SUBPROCESS producer, but reads ``ProjectConfig.manifest_path`` (threaded as
    ``manifest_filepath``) instead of ``{project_dir}/target/manifest.json``: in K8s the pod's
    manifest isn't reachable from the scheduler. The map is mutated in place (never reassigned)
    so the reference held by the pod manager's ``callback_extra_kwargs`` stays valid.

    Degrades to a no-op -- dbt still runs and statuses are still reported, but no datasets are
    emitted -- when generation is disabled, no ``ProfileConfig`` is set, no namespace resolves,
    or the manifest is unavailable.
    """
    operator._model_outlet_uris.clear()
    operator._dataset_namespace = None
    if not operator._should_generate_model_uris:
        return
    # get_dataset_namespace requires a ProfileConfig; some constructions (e.g. inline profiles
    # via profiles_yml_filepath only) don't supply one, so dataset emission degrades to a no-op.
    if operator.profile_config is None:
        return

    from cosmos.dataset import compute_model_outlet_uris, get_dataset_namespace

    operator._dataset_namespace = get_dataset_namespace(operator.profile_config)
    if not operator._dataset_namespace:
        return
    if not operator.manifest_filepath:
        operator.log.warning(
            "manifest_filepath not supplied to %s; per-model dataset emission is disabled for this run. "
            "Pass ProjectConfig.manifest_path to enable it.",
            type(operator).__name__,
        )
        return
    # manifest_filepath is ProjectConfig.manifest_path, an Airflow ObjectStoragePath that may point
    # at a remote manifest (s3://, gs://, ...). Pass it through unchanged -- wrapping it in Path()
    # would mangle remote URIs (e.g. "s3://b/m.json" -> "s3:/b/m.json"). compute_model_outlet_uris
    # reads it via ObjectStoragePath.open() and returns {} (logging) if it's missing or unreadable,
    # so no local existence check is needed here.
    operator._model_outlet_uris.update(
        compute_model_outlet_uris(operator.manifest_filepath, operator._dataset_namespace)
    )


def execute_watcher_producer(operator: Any, context: Context, parent_execute: Callable[..., Any], **kwargs: Any) -> Any:
    """Shared ``execute`` logic for container-based watcher producer operators.

    On retry, restores any XCom backup and raises ``AirflowSkipException`` (the
    producer does not support Airflow retries). On the first attempt, initialises
    an XCom backup, exposes the execution context to the log-parsing callback, runs
    the parent execute, and deletes the backup on success. On failure the producer's
    on-failure/on-retry callback (see ``compose_watcher_backup_callbacks``) flushes
    the backup so the next try can restore it.
    """
    task_instance = context.get("ti")
    if task_instance is None:
        raise AirflowException(f"{type(operator).__name__} expects a task instance in the execution context")

    try_number = getattr(task_instance, "try_number", 1)

    from cosmos import settings

    reliable_retry = settings.enable_watcher_reliable_retry

    if try_number > 1:
        _restore_xcom_from_variable(context)
        raise AirflowSkipException(
            f"{type(operator).__name__} does not support Airflow retries. "
            f"Detected attempt #{try_number}; skipping execution to avoid running a second dbt build."
        )

    _init_xcom_backup(context, persist=reliable_retry)

    # Resolve the namespace and build the per-model outlet URI map before the pod runs, so the
    # log-parsing callback can attach outlet URIs to each model's status XCom for the consumers.
    _populate_producer_model_outlet_uris(operator)
    operator._upstream_failure_skipped_ids.clear()
    # Publish the context through the mutable holder shared by reference with the pod
    # manager's callback_extra_kwargs, so the log-parsing callback sees the live context
    # even if the pod manager (a cached_property) was created before this runs.
    operator._context_holder[CONTEXT_KEY] = context

    safe_xcom_push(task_instance=task_instance, key=_PRODUCER_CMD_FLAGS_XCOM_KEY, value=operator.add_cmd_flags())

    # On failure parent_execute() raises and the on-failure callback flushes the backup.
    return_value = parent_execute(context, **kwargs)
    if reliable_retry:
        _delete_xcom_backup_variable(context)
    return return_value
