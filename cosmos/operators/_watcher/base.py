from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowSkipException

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.constants import (
    _DATASET_EMITTING_RESOURCE_TYPES,
    _DBT_STARTUP_EVENTS_XCOM_KEY,
    AIRFLOW_VERSION,
    CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
)
from cosmos.dbt.graph import DbtNode
from cosmos.listeners.dag_run_listener import EventStatus
from cosmos.log import get_logger
from cosmos.operators._watcher.aggregation import get_tests_status_xcom_key, push_test_result_or_aggregate
from cosmos.operators._watcher.state import (
    _iso_to_string,
    _log_dbt_event,
    build_producer_state_fetcher,
    get_compiled_sql_xcom_key,
    get_dbt_event_xcom_key,
    get_status_xcom_key,
    get_xcom_val,
    is_dbt_node_status_failed,
    is_dbt_node_status_skipped,
    is_dbt_node_status_success,
    is_dbt_node_status_terminal,
    is_dbt_upstream_failure_skip_event,
    is_producer_task_terminated,
    safe_xcom_push,
    xcom_set_lock,
)
from cosmos.operators._watcher.triggerer import WatcherEventReason, WatcherTrigger

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator
    from airflow.sdk.definitions.context import Context
except ImportError:  # pragma: no cover
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.context import Context  # type: ignore[attr-defined]

if TYPE_CHECKING:
    try:
        from airflow.sdk import DAG
    except ImportError:
        from airflow.models.dag import DAG  # type: ignore[assignment]
    from cosmos.airflow.compatibility import EmptyOperator

    try:
        from airflow.sdk import TaskGroup
    except ImportError:
        from airflow.utils.task_group import TaskGroup

logger = get_logger(__name__)

# Subset of dbt event types that represent errors/failures.
# Used (together with node status lifecycle events like NodeStart/NodeCompiling/
# NodeExecuting/NodeFinished) to build _DBT_EVENT_ALLOWLIST, which controls which
# events are surfaced in consumer tasks.
# Source: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/events/types.py
_DBT_ERROR_EVENTS_TYPES = frozenset(
    {
        "InvalidOptionYAML",
        "LogDbtProjectError",
        "LogDbtProfileError",
        "InputFileDiffError",
        "PartialParsingErrorProcessingFile",
        "PartialParsingError",
        "ParsedFileLoadFailed",
        "ParseInlineNodeError",
        "RunningOperationCaughtError",
        "SQLRunnerException",
        "RunningOperationUncaughtError",
        "CatchableExceptionOnRun",
        "InternalErrorOnRun",
        "GenericExceptionOnRun",
        "NodeConnectionReleaseError",
        "MainEncounteredError",
        "RunResultFailure",
        "RunResultError",
        "CheckNodeTestFailure",
        "LogSkipBecauseError",
        "SendEventFailure",
        "FlushEventsFailure",
        "TrackingInitializeFailure",
        "ArtifactUploadError",
    }
)

_DBT_NODE_STATUS_EVENT_TYPES = frozenset({"NodeStart", "NodeCompiling", "NodeExecuting", "NodeFinished"})

_DBT_EVENT_ALLOWLIST = _DBT_ERROR_EVENTS_TYPES | _DBT_NODE_STATUS_EVENT_TYPES


def _process_dbt_log_event(task_instance: Any, dbt_log: dict[str, Any]) -> None:
    """Push a single dbt log event straight to XCom (one write per event, overwriting).

    Retained for callers that do not supply a ``node_event_buffer`` (e.g. the Kubernetes watcher),
    which keep the original behaviour. The local watcher uses the buffered path
    (``_accumulate_dbt_log_event`` + ``_flush_dbt_event``) instead.
    """
    logger.debug("dbt_log: %s", dbt_log)
    data = dbt_log.get("data", {})
    info = dbt_log.get("info", {})

    event_name = info.get("name")
    if event_name not in _DBT_EVENT_ALLOWLIST:
        return None
    node_info = data.get("node_info") or {}
    status = node_info.get("node_status")
    unique_id = node_info.get("unique_id")
    start_time = node_info.get("node_started_at")
    finish_time = node_info.get("node_finished_at")
    # The error text lives in different places depending on the invocation mode: ``run_result.message`` on
    # ``NodeFinished`` (dbt-runner mode) and ``data.msg`` on ``RunResultError`` (subprocess mode).
    run_result = data.get("run_result") or {}
    msg = run_result.get("message") or data.get("msg") or info.get("msg")

    if unique_id:
        dbt_event = {
            "status": status,
            "start_time": _iso_to_string(start_time),
            "finish_time": _iso_to_string(finish_time),
            "msg": msg,
        }

        safe_xcom_push(task_instance=task_instance, key=get_dbt_event_xcom_key(unique_id), value=dbt_event)


# Guards the in-memory per-node event buffer. dbt runner callbacks fire from multiple threads, so
# accumulating into the buffer (and flushing it) must be serialised.
_node_event_buffer_lock = threading.Lock()


def _new_buffer_entry() -> dict[str, Any]:
    return {"status": None, "start_time": None, "finish_time": None, "msg": None, "_has_error_msg": False}


def _accumulate_dbt_log_event(node_event_buffer: dict[str, dict[str, Any]], dbt_log: dict[str, Any]) -> None:
    """Merge a single dbt JSON log event into the producer's in-memory per-node buffer.

    Called for every allowlisted dbt event during the producer run. Nothing is written to XCom here;
    ``_flush_dbt_event`` writes the merged event when the node is terminal (and again if a later error
    event adds the message). This avoids a per-event XCom write (and global-lock contention) for every
    event of every node.

    The error text lives in different places per invocation mode: ``run_result.message`` on
    ``NodeFinished`` (dbt-runner mode) and ``data.msg`` on ``RunResultError`` (subprocess mode). We read
    both, and let an error message win so it is not overwritten by a later, often empty, message.
    """
    info = dbt_log.get("info", {})
    event_name = info.get("name")
    if event_name not in _DBT_EVENT_ALLOWLIST:
        return
    data = dbt_log.get("data", {})
    node_info = data.get("node_info") or {}
    unique_id = node_info.get("unique_id")
    if not unique_id:
        return

    status = node_info.get("node_status")
    start_time = node_info.get("node_started_at")
    finish_time = node_info.get("node_finished_at")
    run_result = data.get("run_result") or {}
    msg = run_result.get("message") or data.get("msg") or info.get("msg")
    is_error = (
        event_name in _DBT_ERROR_EVENTS_TYPES
        or is_dbt_node_status_failed(status)
        or is_dbt_node_status_failed(run_result.get("status"))
    )

    with _node_event_buffer_lock:
        entry = node_event_buffer.setdefault(unique_id, _new_buffer_entry())
        # Some non-lifecycle events (e.g. RunResultError) carry the literal string "None" as
        # node_status; ignore it so it cannot clobber the real terminal status.
        if status and status != "None":
            entry["status"] = status
        if start_time:
            entry["start_time"] = _iso_to_string(start_time)
        if finish_time:
            entry["finish_time"] = _iso_to_string(finish_time)
        if msg:
            # An error message wins and must not be overwritten by a later (often empty or generic)
            # message; a non-error message only fills an as-yet-empty slot.
            if is_error:
                entry["msg"] = msg
                entry["_has_error_msg"] = True
            elif not entry["_has_error_msg"]:
                entry["msg"] = msg


def _flush_dbt_event(
    task_instance: Any,
    node_event_buffer: dict[str, dict[str, Any]],
    unique_id: str,
    terminal_status: str | None = None,
) -> None:
    """Write a node's buffered event to XCom.

    Called when the node reaches a terminal status (with ``terminal_status`` so the authoritative status
    is stamped -- subprocess's status-bearing ``LogModelResult`` is not allowlisted, so the entry may
    otherwise lack one) and again if a later error event adds the message. The entry is intentionally
    not removed: it may be re-flushed, and the whole buffer is cleared at the start of ``execute``.
    """
    with _node_event_buffer_lock:
        entry = node_event_buffer.setdefault(unique_id, _new_buffer_entry())
        if terminal_status and terminal_status != "None":
            entry["status"] = terminal_status
        value = {key: val for key, val in entry.items() if key != "_has_error_msg"}
    safe_xcom_push(task_instance=task_instance, key=get_dbt_event_xcom_key(unique_id), value=value)


def _flush_dbt_event_on_error(
    task_instance: Any, node_event_buffer: dict[str, dict[str, Any]], dbt_log: dict[str, Any]
) -> None:
    """Re-flush a node when an allowlisted error event carrying a message arrives.

    Subprocess mode emits the error text in a ``RunResultError`` event that arrives *after* the terminal
    status event already flushed, so this second flush delivers that message to the consumer.
    """
    info = dbt_log.get("info", {})
    if info.get("name") not in _DBT_ERROR_EVENTS_TYPES:
        return
    data = dbt_log.get("data", {})
    # Mirror the message sources used by _accumulate_dbt_log_event (run_result.message / data.msg /
    # info.msg) so an error whose text is only in info.msg still triggers the re-flush.
    if not ((data.get("run_result") or {}).get("message") or data.get("msg") or info.get("msg")):
        return
    unique_id = (data.get("node_info") or {}).get("unique_id")
    if unique_id:
        _flush_dbt_event(task_instance, node_event_buffer, unique_id)


def _record_dbt_log_event(
    node_event_buffer: dict[str, dict[str, Any]] | None, context: Any, dbt_log: dict[str, Any]
) -> None:
    """Record a dbt log event, choosing the buffered path or the original per-event push.

    Local watcher (``node_event_buffer`` provided): accumulate in memory; the merged event is flushed
    to XCom when the node is terminal, and again here if this is an error event carrying the message
    (subprocess emits it after the terminal event). Otherwise (e.g. the Kubernetes watcher): push per event.
    """
    ti = context.get("ti") if context else None
    if node_event_buffer is not None:
        _accumulate_dbt_log_event(node_event_buffer, dbt_log)
        if ti is not None:
            _flush_dbt_event_on_error(ti, node_event_buffer, dbt_log)
        return
    if ti:
        _process_dbt_log_event(ti, dbt_log)


def _extract_compiled_sql(
    project_dir: str, unique_id: str, node_path: str | None, resource_type: str | None
) -> str | None:
    """
    Extract compiled SQL from the target directory for a given dbt node.

    Used by store_dbt_resource_status_from_log; reads from the target/compiled layout under project_dir.

    Assumes inputs come from dbt (relative node_path, unique_id like model.package.name).
    """
    if resource_type != "model" or not node_path:
        return None

    package = unique_id.split(".", 2)[1]
    compiled_sql_path = Path(project_dir) / "target" / "compiled" / package / "models" / node_path
    if not compiled_sql_path.exists():
        logger.warning(
            "Compiled sql path %s does not exist and hence the rendered template field compiled_sql for the model will not be populated",
            compiled_sql_path,
        )
        return None
    return compiled_sql_path.read_text(encoding="utf-8").strip() or None


def _push_compiled_sql_for_model(task_instance: Any, unique_id: str, compiled_sql: str) -> None:
    """
    Push compiled SQL for a model to XCom under the canonical key.

    Single place where both subprocess and node-event producer paths set compiled_sql.
    Consumers (sensor poke and trigger) always read from this key.
    """
    safe_xcom_push(
        task_instance=task_instance,
        key=get_compiled_sql_xcom_key(unique_id),
        value=compiled_sql,
    )


def store_compiled_sql_for_model(
    task_instance: Any,
    project_dir: str,
    unique_id: str,
    node_path: str | None,
    resource_type: str | None,
) -> None:
    """
    Read compiled SQL from the target directory and store to XCom if present.

    Single sequence used by both subprocess and node-event producer paths.
    """
    compiled_sql = _extract_compiled_sql(project_dir, unique_id, node_path, resource_type)
    if compiled_sql:
        _push_compiled_sql_for_model(task_instance, unique_id, compiled_sql)


def _store_startup_event_from_log(task_instance: Any, log_line: dict[str, Any]) -> None:
    """
    When dbt JSON log contains MainReportVersion or AdapterRegistered, append to
    dbt_startup_events XCom (same shape as runner path) for trigger to log versions.

    The pull+append+push is performed under ``xcom_set_lock`` to prevent a race
    condition: dbt runner callbacks are invoked from multiple threads, so two
    startup events arriving concurrently could both read the same stale list and
    one append would be silently lost.  Holding the same lock used by
    ``safe_xcom_push`` makes the entire read-modify-write atomic.
    """
    event_name = log_line.get("info", {}).get("name")
    if event_name not in ("MainReportVersion", "AdapterRegistered"):
        return
    info = log_line.get("info", {})
    msg = info.get("msg", "")
    ts = info.get("ts", "")
    # Hold the lock for the full read-modify-write cycle.  We call xcom_push
    # directly (bypassing safe_xcom_push) to avoid a deadlock: Lock is not
    # re-entrant, so acquiring it again inside safe_xcom_push would block forever.
    with xcom_set_lock:
        current = list(task_instance.xcom_pull(key=_DBT_STARTUP_EVENTS_XCOM_KEY) or [])
        current.append({"name": event_name, "msg": msg, "ts": ts})
        task_instance.xcom_push(key=_DBT_STARTUP_EVENTS_XCOM_KEY, value=current)


def _log_dbt_msg(log_line: dict[str, Any]) -> None:
    """Log the human-readable message from a parsed dbt JSON log line."""
    log_info = log_line.get("info", {})
    msg = log_info.get("msg")
    if msg is None:
        return
    level = log_info.get("level", "INFO").upper()
    ts = log_info.get("ts")
    formatted_ts = _iso_to_string(ts)
    if formatted_ts:
        logger.log(getattr(logging, level, logging.INFO), "%s  %s", formatted_ts, msg)
    else:
        logger.log(getattr(logging, level, logging.INFO), msg)


_model_outlet_uris_lock = threading.Lock()


_MODEL_OUTLET_URIS_ATTEMPTED_KEY = "__attempted__"


def _ensure_subprocess_model_outlet_uris(
    model_outlet_uris: dict[str, list[str]] | None,
    dataset_namespace: str | None,
    project_dir: str | None,
) -> None:
    """Lazily populate model_outlet_uris from the manifest when first needed.

    Thread-safe: in DBT_RUNNER mode the log parser callback can be invoked from
    multiple dbt threads. The lock prevents duplicate manifest reads and ensures
    the dict is fully populated before any reader accesses it.

    A sentinel key is set after the attempt completes so that an empty manifest
    result does not cause repeated filesystem reads.
    """
    if model_outlet_uris is None or not dataset_namespace or not project_dir:
        return
    with _model_outlet_uris_lock:
        if _MODEL_OUTLET_URIS_ATTEMPTED_KEY in model_outlet_uris:
            return
        from cosmos.dataset import compute_model_outlet_uris

        manifest_path = Path(project_dir) / "target" / "manifest.json"
        if manifest_path.exists():
            model_outlet_uris.update(compute_model_outlet_uris(manifest_path, dataset_namespace))
        model_outlet_uris[_MODEL_OUTLET_URIS_ATTEMPTED_KEY] = []


def _rewrite_upstream_failure_skip_status(
    log_line: dict[str, Any],
    unique_id: str | None,
    dbt_node_status: Any,
    upstream_failure_skipped_ids: set[str] | None,
) -> Any:
    """Track upstream-failure-skipped nodes and rewrite their status from "skipped" to "failed".

    dbt emits two events for a node that it skipped because of an upstream
    failure: ``SkippingDetails``/``LogSkipBecauseError`` during ``on_skip()``,
    and a later ``NodeFinished`` from the runnable ``finally``-block -- both
    carry ``node_status="skipped"``. The first identifies the upstream-failure
    cause (those events are reached only via ``do_skip(cause=...)``, exclusively
    on upstream-node failure); the second would otherwise overwrite the
    rewritten XCom with the original ``"skipped"``. Tracking affected
    ``unique_id``\\ s in a per-execution set lets the parser rewrite both. See
    #2698.
    """
    if not unique_id or upstream_failure_skipped_ids is None:
        return dbt_node_status
    if is_dbt_upstream_failure_skip_event(log_line.get("info", {}).get("name")):
        upstream_failure_skipped_ids.add(unique_id)
    if dbt_node_status == "skipped" and unique_id in upstream_failure_skipped_ids:
        return "failed"
    return dbt_node_status


def _surface_non_json_stdout(line: str) -> None:
    """Forward non-JSON stdout (e.g. Snowflake connector's "Going to open: <URL>" prompt
    during externalbrowser auth) to the task log instead of silently dropping it.
    """
    if line:
        logger.info("%s", line)


def store_dbt_resource_status_from_log(
    line: str,
    extra_kwargs: Any,
    *,
    tests_per_model: dict[str, list[str]] | None = None,
    test_results_per_model: dict[str, dict[str, str]] | None = None,
    model_outlet_uris: dict[str, list[str]] | None = None,
    dataset_namespace: str | None = None,
    should_generate_model_uris: bool = True,
    upstream_failure_skipped_ids: set[str] | None = None,
    node_event_buffer: dict[str, dict[str, Any]] | None = None,
) -> None:
    """
    Parses a single line from dbt JSON logs and stores node status to Airflow XCom.

    This method parses each log line from dbt when --log-format json is used,
    extracts node status information, and pushes it to XCom for consumption
    by downstream watcher sensors.

    :param line: A single line from dbt JSON logs.
    :param extra_kwargs: Additional keywords arguments.
    :param tests_per_model: Mapping of model unique_id to list of test unique_ids
        associated with that model, as built by DbtGraph.update_node_dependency().
        Empty dict when no tests exist.
    :param test_results_per_model: Mutable accumulator dict. For each model that has
        tests, collects the terminal statuses of those tests as they finish.
        Keyed by model unique_id; values are dicts mapping each test's unique_id to its
        status (e.g. ``{"test.pkg.not_null_orders_id": "pass"}``), so a replayed log line
        for the same test is idempotent. Mutated in place by this function.
    :param model_outlet_uris: Mutable dict mapping unique_id to outlet URIs.
        Populated lazily from the manifest on first terminal status detection.
    :param dataset_namespace: The OL-compatible dataset namespace for URI construction.
    :param should_generate_model_uris: Explicit control for whether per-model outlet URIs are
        computed from the manifest. When ``False`` (e.g. consumers won't emit datasets), URI
        generation is skipped regardless of ``dataset_namespace``.
    :param upstream_failure_skipped_ids: Mutable accumulator set of node unique_ids
        that dbt skipped because of an upstream-node failure. Populated when this
        function sees a ``SkippingDetails`` or ``LogSkipBecauseError`` event; later
        ``NodeFinished`` events with ``node_status="skipped"`` for these unique_ids
        are rewritten to ``"failed"`` so the consumer sensor fails (and Airflow can
        retry it) rather than going SKIPPED. See #2698.
    :param node_event_buffer: Mutable in-memory accumulator keyed by node unique_id. Each dbt event
        is merged here (no XCom write); the merged event is flushed to XCom when the node reaches a
        terminal state and may be re-flushed if a later error event provides the message (e.g. subprocess mode).
        This replaces the previous per-event XCom push, which wrote on every event (lock contention) and let a trailing ``NodeFinished`` overwrite the captured error message.
    """
    # extra_kwargs is typed Any and may be falsy/None; normalise so the .get() calls below are safe.
    extra_kwargs = extra_kwargs or {}
    try:
        log_line = json.loads(line)
    except json.JSONDecodeError:
        _surface_non_json_stdout(line)
        log_line = {}
    else:
        context = extra_kwargs.get("context")
        _record_dbt_log_event(node_event_buffer, context, log_line)
        if context is not None:
            _store_startup_event_from_log(context["ti"], log_line)
        node_info = log_line.get("data", {}).get("node_info", {})
        dbt_node_status = node_info.get("node_status")
        dbt_node_resource_type = node_info.get("resource_type")
        unique_id = node_info.get("unique_id")

        # Rewrite "skipped" to "failed" when dbt skipped the node because an
        # upstream node failed (#2698). See _rewrite_upstream_failure_skip_status.
        dbt_node_status = _rewrite_upstream_failure_skip_status(
            log_line, unique_id, dbt_node_status, upstream_failure_skipped_ids
        )

        logger.debug("Model: %s is in %s state", unique_id, dbt_node_status)

        if is_dbt_node_status_terminal(dbt_node_status):
            context = extra_kwargs.get("context")
            if context is None:
                logger.warning(
                    "context is None for terminal node '%s' — XCom status will not be pushed. "
                    "This is unexpected and should never happen; check the caller is passing context correctly.",
                    unique_id,
                )
                return
            if dbt_node_resource_type == "test" and tests_per_model and test_results_per_model is not None:
                logger.debug("Test '%s' finished with status '%s'", unique_id, dbt_node_status)
                push_test_result_or_aggregate(
                    unique_id, dbt_node_status, tests_per_model, test_results_per_model, context["ti"]
                )
            else:
                # Lazily populate per-model outlet URIs from the manifest, but only when the
                # producer is configured to generate them and the resource type can emit datasets
                # (models/seeds/snapshots).
                outlet_uris: list[str] = []
                if should_generate_model_uris and dbt_node_resource_type in _DATASET_EMITTING_RESOURCE_TYPES:
                    project_dir = extra_kwargs.get("project_dir")
                    _ensure_subprocess_model_outlet_uris(model_outlet_uris, dataset_namespace, project_dir)
                    outlet_uris = model_outlet_uris.get(unique_id, []) if model_outlet_uris else []

                status_value: dict[str, Any] | str = {
                    "status": dbt_node_status,
                    "outlet_uris": outlet_uris,
                }
                safe_xcom_push(
                    task_instance=context["ti"],
                    key=get_status_xcom_key(unique_id),
                    value=status_value,
                )

            # Extract and push compiled_sql for models (centralised for both subprocess and node-event)
            # compiled_sql is available for both success and failed models - it's compiled before execution
            project_dir = extra_kwargs.get("project_dir")
            if project_dir:
                store_compiled_sql_for_model(
                    context["ti"], project_dir, unique_id, node_info.get("node_path"), node_info.get("resource_type")
                )

            # Flush this node's buffered structured event now that it is terminal, stamping the
            # authoritative terminal status (subprocess's status-bearing event is not allowlisted).
            if node_event_buffer is not None and unique_id:
                _flush_dbt_event(context["ti"], node_event_buffer, unique_id, terminal_status=dbt_node_status)

    # Additionally, log the message from dbt logs
    _log_dbt_msg(log_line)


# dbt flags set by the producer that must not propagate into the consumer's
# retry command. ``--select`` / ``--exclude`` are dropped because the consumer
# targets a single model and re-applies its own selector; ``--log-format`` is
# dropped because the consumer's retry is a user-facing dbt run and should
# default to text. Each entry consumes the following token as its value (e.g.
# ``--log-format json``).
_PRODUCER_ONLY_FLAGS: tuple[str, ...] = ("--select", "--exclude", "--log-format")


class BaseConsumerSensor(BaseSensorOperator):
    template_fields: tuple[str, ...] = ("model_unique_id", "compiled_sql")
    poke_retry_number: int = 0

    def __init__(
        self,
        *,
        project_dir: str,
        profile_config: ProfileConfig | None = None,
        profiles_dir: str | None = None,
        producer_task_id: str = PRODUCER_WATCHER_TASK_ID,
        poke_interval: int = 10,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        self.compiled_sql = ""
        kwargs.setdefault("priority_weight", CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WATCHER_TASK_WEIGHT_RULE)

        # We pop extra_context before super().__init__ because BaseSensorOperator does not accept it and
        # would raise on unknown kwargs; we assign after so subclasses and user customisations can use it
        # at runtime (e.g. in execute, poke, or callbacks).
        extra_context = kwargs.pop("extra_context") if "extra_context" in kwargs else {}
        super().__init__(
            poke_interval=poke_interval,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            **kwargs,
        )
        self.extra_context = extra_context

        self.project_dir = project_dir
        self.producer_task_id = producer_task_id
        self.deferrable = deferrable
        self.model_unique_id = extra_context.get("dbt_node_config", {}).get("unique_id")

    @property
    def is_test_sensor(self) -> bool:
        """Whether this sensor watches aggregated test results instead of individual node results."""
        return False

    @property
    def _resource_label(self) -> str:
        """Human-readable label for log and error messages."""
        return "Tests for model" if self.is_test_sensor else "Model"

    @staticmethod
    def _filter_flags(flags: list[str]) -> list[str]:
        """Filter out dbt flags that should not propagate from the producer to the consumer's retry.

        The set of stripped flags is defined by ``_PRODUCER_ONLY_FLAGS`` at module
        scope; see that constant's docstring for the rationale per flag.
        """
        filtered = []
        skip_next = False
        for token in flags:
            if skip_next:
                if token.startswith("--"):
                    skip_next = False
                else:
                    continue  # skip value of previous flag
            if token in _PRODUCER_ONLY_FLAGS:
                skip_next = True
                continue
            filtered.append(token)
        return filtered

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """
        Handles logic for retrying a failed dbt model execution.
        Reconstructs the dbt command by cloning the project and re-running the model
        with appropriate flags, while ensuring flags like ``--select``, ``--exclude``,
        and ``--log-format`` (the producer-only JSON event stream) are excluded.

        Users who want JSON-formatted dbt output on retry can opt in by passing
        ``operator_args={"dbt_cmd_flags": ["--log-format", "json"]}`` to their
        ``DbtDag`` / ``DbtTaskGroup``; ``dbt_cmd_flags`` is appended automatically
        by ``build_cmd`` and is not filtered here.

        Subclasses may override to issue a different dbt subcommand (e.g. ``dbt test``
        for test sensors or ``dbt source freshness`` for source sensors).
        """
        if try_number > 1:
            logger.info(
                "Retry attempt #%s – Running model '%s' from project '%s' using %s",
                try_number - 1,
                self.model_unique_id,
                self.project_dir,
                self.__class__.__name__,
            )
        else:
            logger.info(
                "Falling back to running model '%s' from project '%s' using %s",
                self.model_unique_id,
                self.project_dir,
                self.__class__.__name__,
            )

        upstream_task = context["ti"].task.dag.get_task(self.producer_task_id)

        extra_flags: list[str] = []
        if upstream_task and hasattr(upstream_task, "add_cmd_flags"):
            raw_flags = upstream_task.add_cmd_flags()
            extra_flags = self._filter_flags(raw_flags)

        model_selector = DbtNode.get_resource_name_from_unique_id(self.model_unique_id)
        cmd_flags = extra_flags + ["--select", model_selector]

        self.build_and_run_cmd(context, cmd_flags=cmd_flags)  # type: ignore[attr-defined]

        logger.info("dbt run completed successfully on retry for model '%s'", self.model_unique_id)
        return True

    def _get_producer_task_status(self, context: Context) -> str | None:
        """
        Get the task status of the producer task for both Airflow 2 and Airflow 3.

        Returns the state of the producer task instance, or None if not found.
        """
        ti = context["ti"]
        run_id = context["run_id"]
        dag_id = ti.dag_id

        fetch_state = build_producer_state_fetcher(
            airflow_version=AIRFLOW_VERSION,
            dag_id=dag_id,
            run_id=run_id,
            producer_task_id=self.producer_task_id,
            logger=logger,
        )
        if fetch_state is None:
            return None

        return fetch_state()

    def _execute_core(self, context: Context) -> None:
        """Run or defer the sensor. Extracted so execute() can wrap it with debug tracking."""
        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context):
            xcom_key = (
                get_tests_status_xcom_key(self.model_unique_id)
                if self.is_test_sensor
                else get_status_xcom_key(self.model_unique_id)
            )
            logger.info(
                "Deferring %s '%s'. The trigger will poll XCom key '%s' from producer task '%s'.",
                self._resource_label.lower(),
                self.model_unique_id,
                xcom_key,
                self.producer_task_id,
            )
            self.defer(
                trigger=WatcherTrigger(
                    model_unique_id=self.model_unique_id,
                    producer_task_id=self.producer_task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    map_index=context["task_instance"].map_index,
                    poke_interval=self.poke_interval,
                    is_test_sensor=self.is_test_sensor,
                ),
                timeout=self.execution_timeout,
                method_name=self.execute_complete.__name__,
            )

    def execute(self, context: Context, **kwargs: Any) -> None:
        if settings.enable_debug_mode:
            from cosmos.debug import start_memory_tracking, stop_memory_tracking

            start_memory_tracking(context)
            try:
                self._execute_core(context)
            finally:
                # Use finally (not except Exception) because self.defer() raises TaskDeferred,
                # a BaseException subclass, which would bypass an except-Exception block and
                # leave the tracker running. Deferring is normal execution, not an error.
                stop_memory_tracking(context)
        else:
            self._execute_core(context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        status = event.get("status")
        reason = event.get("reason")

        # Log the node's dbt event once at terminal (poke() does the same for the non-deferrable path).
        dbt_events = get_xcom_val(
            task_instance=context["ti"],
            key=get_dbt_event_xcom_key(self.model_unique_id),
            task_ids=self.producer_task_id,
        )
        _log_dbt_event(dbt_events)

        if status == EventStatus.SKIPPED:
            raise AirflowSkipException(
                f"{self._resource_label} '{self.model_unique_id}' was skipped by the dbt command."
            )

        if status == "success" and reason == WatcherEventReason.NODE_NOT_RUN:
            logger.info(
                "%s '%s' was skipped by the dbt command. This may happen if it is an ephemeral model or if the model sql file is empty.",
                self._resource_label,
                self.model_unique_id,
            )

        # Extract and store compiled_sql from the event if available
        compiled_sql = event.get("compiled_sql")
        if compiled_sql:
            self.compiled_sql = compiled_sql
            if hasattr(self, "_override_rtif"):
                self._override_rtif(context)

        if status != "failed":
            return

        if reason == WatcherEventReason.NODE_FAILED:
            raise AirflowException(
                f"dbt {self._resource_label.lower()} '{self.model_unique_id}' failed. Review the producer task '{self.producer_task_id}' logs for details."
            )

        if reason == WatcherEventReason.PRODUCER_FAILED:
            raise AirflowException(
                f"Watcher producer task '{self.producer_task_id}' failed before reporting results for {self._resource_label.lower()} '{self.model_unique_id}'. Check its logs for the underlying error."
            )

        if reason == WatcherEventReason.PRODUCER_SKIPPED:
            logger.info(
                "Watcher producer task '%s' was skipped. Falling back to running dbt for %s '%s'.",
                self.producer_task_id,
                self._resource_label.lower(),
                self.model_unique_id,
            )
            self._fallback_to_non_watcher_run(try_number=context["ti"].try_number, context=context)
            return

    def _log_startup_events(self, ti: Any) -> None:
        dbt_startup_events: list[dict[str, Any]] = ti.xcom_pull(
            task_ids=self.producer_task_id, key=_DBT_STARTUP_EVENTS_XCOM_KEY
        )
        if isinstance(dbt_startup_events, list) and dbt_startup_events:  # pragma: no cover
            for event in dbt_startup_events:
                # Adding debug level to avoid redundant logs for non-deferrable mode
                logger.debug("%s", event.get("msg"))

    def _get_node_status(self, ti: Any, context: Context) -> Any:
        """Return the current status of the watched dbt node from XCom.

        For test sensors, reads the aggregated ``_tests_status`` key.
        For model sensors, reads the per-model ``*_status`` key (same for both
        SUBPROCESS and DBT_RUNNER invocation modes). The value is always a dict
        with ``status`` and ``outlet_uris`` keys.

        Side effect: stores outlet URIs on ``self._outlet_uris`` for later
        dataset emission.
        """
        if self.is_test_sensor:
            return get_xcom_val(ti, self.producer_task_id, get_tests_status_xcom_key(self.model_unique_id))
        xcom_val = get_xcom_val(ti, self.producer_task_id, get_status_xcom_key(self.model_unique_id))
        if xcom_val is None:
            return None
        self._outlet_uris = xcom_val.get("outlet_uris", [])
        return xcom_val.get("status")

    def _cache_compiled_sql(self, ti: Any, context: Context) -> None:
        """Pull compiled_sql from XCom and cache it on the sensor instance."""
        compiled_sql = get_xcom_val(ti, self.producer_task_id, get_compiled_sql_xcom_key(self.model_unique_id))
        if compiled_sql:
            self.compiled_sql = compiled_sql
            if hasattr(self, "_override_rtif"):
                self._override_rtif(context)

    def _handle_retry(self, try_number: int, producer_task_state: str | None, context: Context) -> bool | None:
        """Handle sensor retry by checking whether the producer is still active.

        Returns the fallback result if the producer has terminated, or None if
        the sensor should continue polling (producer still active).
        """
        if is_producer_task_terminated(producer_task_state):
            # Producer finished — this is either an automatic retry after
            # the producer completed or a manual task clear from the UI.
            # Fall back to a non-watcher run.
            return self._fallback_to_non_watcher_run(try_number, context)
        # Producer is still active — the sensor likely timed out while the
        # producer was still working.  Keep polling instead of launching a
        # duplicate dbt run.
        logger.info(
            "Try #%s but producer '%s' is still %s — continuing to poll instead of fallback.",
            try_number,
            self.producer_task_id,
            producer_task_state or "unknown",
        )
        return None

    def _handle_no_dbt_node_status(self, producer_task_state: str | None, try_number: int, context: Context) -> bool:
        """Handle the case where no dbt node status has been reported yet."""
        if producer_task_state == "failed":
            if self.poke_retry_number > 0:
                raise AirflowException(
                    f"The dbt build command failed in producer task. Please check the log of task {self.producer_task_id} for details."
                )
            else:
                # This handles the scenario of tasks that failed with `State.UPSTREAM_FAILED`
                return self._fallback_to_non_watcher_run(try_number, context)

        if producer_task_state == "skipped":
            return self._fallback_to_non_watcher_run(try_number, context)

        self.poke_retry_number += 1
        return False

    def poke(self, context: Context) -> bool:
        """
        Checks the status of a dbt node (model or aggregated tests) by pulling relevant XComs from the producer task.
        Handles retries and checks for successful completion.
        """
        ti = context["ti"]
        try_number = ti.try_number

        xcom_key = (
            get_tests_status_xcom_key(self.model_unique_id)
            if self.is_test_sensor
            else get_status_xcom_key(self.model_unique_id)
        )
        logger.info(
            "Try number #%s, poke attempt #%s: Pulling status from task_id '%s' via XCom key '%s' for %s '%s'",
            try_number,
            self.poke_retry_number,
            self.producer_task_id,
            xcom_key,
            self._resource_label.lower(),
            self.model_unique_id,
        )

        producer_task_state = self._get_producer_task_status(context)

        if try_number > 1:
            retry_result = self._handle_retry(try_number, producer_task_state, context)
            if retry_result is not None:
                return retry_result

        if not self.is_test_sensor:
            self._log_startup_events(ti)
        status = self._get_node_status(ti, context)

        # compiled_sql is always in the canonical per-model XCom key (same for event and subprocess modes)
        if status is not None:
            self._cache_compiled_sql(ti, context)

        if status is None:
            return self._handle_no_dbt_node_status(producer_task_state, try_number, context)

        # Log the dbt event only once the node is terminal; poke runs every interval, so logging before
        # this point would repeat the line on each poke.
        dbt_events = get_xcom_val(
            task_instance=context["ti"],
            key=get_dbt_event_xcom_key(self.model_unique_id),
            task_ids=self.producer_task_id,
        )
        _log_dbt_event(dbt_events)

        if is_dbt_node_status_skipped(status):
            raise AirflowSkipException(
                f"{self._resource_label} '{self.model_unique_id}' was skipped by the dbt command."
            )
        elif is_dbt_node_status_success(status):
            return True
        else:
            raise AirflowException(f"{self._resource_label} '{self.model_unique_id}' finished with status '{status}'")


def create_producer_done_task(dag: DAG, task_group: TaskGroup, task_id: str) -> EmptyOperator:
    """Create an EmptyOperator that absorbs the producer's skip state on retry.

    This task sits downstream of the producer inside the DbtTaskGroup. When the producer
    is skipped on retry, this task still succeeds (trigger_rule=NONE_FAILED), preventing
    the skip from propagating to tasks downstream of the group.
    """
    from cosmos.airflow.compatibility import EmptyOperator

    try:
        from airflow.task.trigger_rule import TriggerRule
    except ImportError:
        from airflow.utils.trigger_rule import TriggerRule

    return EmptyOperator(
        task_id=task_id,
        dag=dag,
        task_group=task_group,
        trigger_rule=TriggerRule.NONE_FAILED,
        doc_md=(
            "**Cosmos internal task.** Absorbs the producer's skip state on retry "
            "so it does not propagate to tasks downstream of this DbtTaskGroup."
        ),
    )
