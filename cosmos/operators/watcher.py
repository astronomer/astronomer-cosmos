from __future__ import annotations

import base64
import functools
import json
import zlib
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos.config import ProfileConfig
from cosmos.dbt.graph import DbtNode
from cosmos.operators._watcher import _parse_compressed_xcom, safe_xcom_push
from cosmos.operators._watcher.state import DBT_FAILED_STATUSES
from cosmos.settings import watcher_dbt_execution_queue

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.constants import (
    _DBT_STARTUP_EVENTS_XCOM_KEY,
    PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
    DbtResourceType,
    InvocationMode,
)
from cosmos.log import get_logger
from cosmos.operators._watcher.aggregation import push_test_result_or_aggregate
from cosmos.operators._watcher.base import (
    BaseConsumerSensor,
    _process_dbt_log_event,
    store_compiled_sql_for_model,
    store_dbt_resource_status_from_log,
)
from cosmos.operators.base import (
    DbtBuildMixin,
    DbtRunMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
)
from cosmos.operators.local import (
    DbtLocalBaseOperator,
    DbtRunLocalOperator,
    DbtSourceLocalOperator,
)

try:
    from dbt_common.events.base_types import EventMsg
except ImportError:  # pragma: no cover
    EventMsg = None


if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_QUEUE
except ImportError:  # pragma: no cover
    from airflow.models.abstractoperator import DEFAULT_QUEUE  # type: ignore[no-redef]

logger = get_logger(__name__)


def _default_freshness_callback(
    context: Context,
    dag: Any,
    task_group: TaskGroup | None,
    nodes: dict[str, DbtNode] | None,
    sources_json: dict[str, Any] | None,
) -> tuple[list[str], str]:
    """Return unique_ids of all nodes that transitively depend on a stale source, plus the status ``"skip"``.

    Stale sources are those with ``status`` of ``"error"`` or ``"warn"`` in ``sources_json["results"]``.
    Traversal is BFS over the reverse-dependency graph built from ``nodes``.
    """
    if not nodes or not sources_json:
        return [], "skip"

    stale_source_ids = {r["unique_id"] for r in sources_json.get("results", []) if r.get("status") in ("error", "warn")}
    if not stale_source_ids:
        return [], "skip"

    # Build reverse map: dep_id -> set of node_ids that directly depend on it
    dependents: dict[str, set[str]] = {}
    for node_id, node in nodes.items():
        for dep_id in node.depends_on:
            dependents.setdefault(dep_id, set()).add(node_id)

    # BFS from each stale source to collect all transitive dependents
    _excludable_resource_types = {DbtResourceType.MODEL, DbtResourceType.SEED, DbtResourceType.SNAPSHOT}
    visited: set[str] = set()
    queue = list(stale_source_ids)
    while queue:
        current = queue.pop()
        for dependent_id in dependents.get(current, set()):
            if dependent_id not in visited:
                visited.add(dependent_id)
                queue.append(dependent_id)

    # Only return model/seed/snapshot nodes — tests are skipped automatically when their parent is excluded,
    # and test hash-suffixed unique_ids are not valid dbt --exclude selectors.
    excludable = [uid for uid in visited if nodes.get(uid) and nodes[uid].resource_type in _excludable_resource_types]
    logger.info("Nodes to skip due to stale sources: %s", excludable)
    return excludable, "skip"


class DbtProducerWatcherOperator(DbtBuildMixin, DbtLocalBaseOperator):
    """Run dbt build and update XCom with the progress of each model, as part of the *WATCHER* execution mode.

    Executes **one** ``dbt build`` covering the whole selection.

    - **When ``InvocationMode.DBT_RUNNER`` is set** we patch
      ``dbtRunner`` so we receive structured events *while* dbt is running.  In
      this real-time mode the operator:
        – pushes startup metadata events (``MainReportVersion``,
          ``AdapterRegistered``) together under XCom key
          ``dbt_startup_events``;
        – pushes each ``NodeFinished`` event immediately to XCom under
          ``nodefinished_<unique_id>`` (zlib zipped+base64 JSON) so downstream
          sensors can react with near-zero latency.

    - **When ``dbtRunner`` is *not* available** (older dbt or
      ``InvocationMode=SUBPROCESS``) we fallback to delayed strategy: after
      dbt exits we read ``target/run_results.json`` and push the whole mapping
      once under key ``run_results`` to XCom.  Sensors can poll this key but will not
      get per-model updates until the build completes - by the end of the execution of all dbt nodes.

    This keeps the heavy dbt work centralised while providing near real-time
    feedback and granular task-level observability downstream.

    When ``check_source_freshness=True`` is passed (via ``operator_args``), the producer first runs
    ``dbt source freshness``, reads ``target/sources.json``, and for every stale source:

    - pushes a synthetic ``"skipped"`` XCom entry for every model that transitively depends on that
      source so consumer sensors raise ``AirflowSkipException`` instead of waiting;
    - adds those models to the ``--exclude`` list of the subsequent ``dbt build`` call.
    """

    template_fields = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]
    # Use staticmethod to prevent Python's descriptor protocol from binding the function to `self`
    # when accessed via instance, which would incorrectly pass `self` as the first argument
    _process_log_line_callable: Callable[[str, Any], None] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self.tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self.test_results_per_model: dict[str, list[str]] = {}
        self._check_source_freshness: bool = kwargs.pop("_check_source_freshness", False)
        self._freshness_callback: Callable[
            [Context, Any, TaskGroup | None, dict[str, DbtNode] | None, dict[str, Any] | None],
            tuple[list[str], str],
        ] = _default_freshness_callback
        # Do not publish compiled_sql to the producer's rendered_template: it would contain SQL for
        # all models run by the producer, is often truncated in the UI due to size, and is of no use
        # there; individual sensor tasks show the corresponding rendered_template per model.
        kwargs["should_store_compiled_sql"] = False
        kwargs.setdefault("priority_weight", PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WATCHER_TASK_WEIGHT_RULE)
        # Consumer watcher retry logic handles model-level reruns using the LOCAL execution mode; rerunning the producer
        # would repeat the full dbt build and duplicate watcher callbacks which may not be processed by the consumers if
        # they have already processed output XCOMs from the first run of the producer, so we disable retries.
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0
        kwargs["queue"] = watcher_dbt_execution_queue or kwargs.get("queue") or DEFAULT_QUEUE
        super().__init__(task_id=task_id, *args, **kwargs)

        if self.invocation_mode == InvocationMode.SUBPROCESS:
            self.log_format = "json"

    @staticmethod
    def _serialize_event(event_message: EventMsg) -> dict[str, Any]:
        """Convert structured dbt EventMsg to plain dict."""
        from google.protobuf.json_format import MessageToDict

        return MessageToDict(event_message, preserving_proto_field_name=True)  # type: ignore[no-any-return]

    def _handle_startup_event(self, event_message: EventMsg, startup_events: list[dict[str, Any]]) -> None:
        info = event_message.info  # type: ignore[attr-defined]
        raw_ts = getattr(info, "ts", None)
        ts_val = raw_ts.ToJsonString() if hasattr(raw_ts, "ToJsonString") else str(raw_ts)  # type: ignore[union-attr]
        startup_events.append({"name": info.name, "msg": info.msg, "ts": ts_val})

    def _handle_node_finished(
        self,
        event_message: EventMsg,
        context: Context,
    ) -> None:
        logger.debug("DbtProducerWatcherOperator: handling node finished event: %s", event_message)
        uid = event_message.data.node_info.unique_id
        node_path_val = getattr(event_message.data.node_info, "node_path", None)
        node_path = str(node_path_val) if node_path_val is not None else None
        resource_type = getattr(event_message.data.node_info, "resource_type", None)
        event_message_dict = self._serialize_event(event_message)
        store_compiled_sql_for_model(context["ti"], self.project_dir, uid, node_path, resource_type)

        if resource_type == "test" and self.tests_per_model:
            status: str = getattr(event_message.data.node_info, "node_status", None) or ""
            logger.debug("Test '%s' finished with status '%s'", uid, status)
            push_test_result_or_aggregate(uid, status, self.tests_per_model, self.test_results_per_model, context["ti"])
        else:
            payload = base64.b64encode(zlib.compress(json.dumps(event_message_dict).encode())).decode()
            safe_xcom_push(task_instance=context["ti"], key=f"nodefinished_{uid.replace('.', '__')}", value=payload)

    def _finalize(self, context: Context, startup_events: list[dict[str, Any]]) -> None:
        # Only push startup events; per-model statuses are available via individual nodefinished_<uid> entries.
        if startup_events:
            safe_xcom_push(task_instance=context["ti"], key=_DBT_STARTUP_EVENTS_XCOM_KEY, value=startup_events)

    def _push_skipped_xcom_for_model(self, ti: Any, unique_id: str) -> None:
        """Push a synthetic ``"skipped"`` status XCom for a model excluded due to a stale upstream source.

        Uses the same XCom key and payload format that consumer sensors already poll so that no
        changes are required on the consumer side beyond recognising the new ``"skipped"`` status.

        - **DBT_RUNNER mode**: key ``nodefinished_<uid>``, value is a compressed+base64 JSON payload
          matching the structure that ``_get_status_from_events`` unpacks.
        - **SUBPROCESS mode**: key ``<uid>_status``, value is the plain string ``"skipped"``.
        """
        uid_key = unique_id.replace(".", "__")
        if self.invocation_mode == InvocationMode.DBT_RUNNER:
            payload = base64.b64encode(
                zlib.compress(json.dumps({"data": {"run_result": {"status": "skipped"}}}).encode())
            ).decode()
            safe_xcom_push(task_instance=ti, key=f"nodefinished_{uid_key}", value=payload)
        else:
            safe_xcom_push(task_instance=ti, key=f"{uid_key}_status", value="skipped")

    def _run_source_freshness(self, context: Context) -> None:
        """Run ``dbt source freshness`` via ``build_cmd`` and ``run_command`` (temp dir, profile, deps like ``dbt build``)."""
        original_base_cmd = self.base_cmd
        original_indirect_selection = getattr(self, "indirect_selection", None)
        try:
            self.base_cmd = ["source", "freshness"]
            self.indirect_selection = None  # ``dbt source freshness`` does not support --indirect-selection
            full_cmd, env = self.build_cmd(context=context, cmd_flags=[])
            self.run_command(cmd=full_cmd, env=env, context=context)
        finally:
            self.base_cmd = original_base_cmd
            self.indirect_selection = original_indirect_selection

    def _skipped_node_token(self, context: Context, node_unique_ids: list[str]) -> None:
        if not node_unique_ids:
            return

        ti = context["ti"]

        for unique_id in node_unique_ids:
            logger.info(
                "Marking resource '%s' as skipped (stale upstream source)",
                unique_id,
            )
            self._push_skipped_xcom_for_model(ti, unique_id)

        model_names = {uid.rsplit(".", 1)[-1] for uid in node_unique_ids}

        current_exclude = getattr(self, "exclude", None)

        if isinstance(current_exclude, str):
            current_set = set(current_exclude.split())
        elif isinstance(current_exclude, (list, set, tuple)):
            current_set = set(current_exclude)
        else:
            current_set = set()

        updated_exclude = current_set | model_names
        self.exclude = " ".join(sorted(updated_exclude))

        logger.info("dbt build --exclude updated: %s", self.exclude)

    def _apply_source_freshness(self, context: Context) -> None:
        """Run freshness, filter stale sources via callback, then skip XCom + ``--exclude`` for downstream resources."""
        try:
            self._run_source_freshness(context)
            logger.info("self.dag.__dict__: %s", self.dag.__dict__)
            dbt_nodes = getattr(getattr(getattr(self, "dag", None), "dbt_graph", None), "nodes", None)
            node_unique_ids, status = self._freshness_callback(
                context, self.dag, self.task_group, dbt_nodes, self._sources_json
            )
            if node_unique_ids is None or node_unique_ids == []:
                return
            if status == "skip":
                self._skipped_node_token(context, node_unique_ids)
        except Exception:  # intentional: freshness check must never block dbt build
            logger.exception("Unexpected error during source freshness check; proceeding with full dbt build.")
            return

    def _set_invocation_mode_if_not_set(self) -> None:
        if not self.invocation_mode:
            logger.info("No invocation mode provided, discovering it")
            self._discover_invocation_mode()

    def _set_process_log_line_callable_if_subprocess(self) -> None:
        if self.invocation_mode == InvocationMode.SUBPROCESS:
            logger.info(
                "DbtProducerWatcherOperator: Setting log_format to json and process_log_line_callable to store_dbt_resource_status_from_log"
            )
            self.log_format = "json"
            self._process_log_line_callable = functools.partial(
                store_dbt_resource_status_from_log,
                tests_per_model=self.tests_per_model,
                test_results_per_model=self.test_results_per_model,
            )

    def execute(self, context: Context, **kwargs: Any) -> Any:
        self._set_invocation_mode_if_not_set()
        self._set_process_log_line_callable_if_subprocess()

        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("DbtProducerWatcherOperator expects a task instance in the execution context")

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            self.log.info(
                "Dbt WATCHER producer task does not support Airflow retries. "
                "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
                try_number,
            )
            return None

        self.log.info(
            "Dbt WATCHER producer task forces Airflow retries to 0 so the dbt build only runs once; "
            "downstream sensors own model-level retries."
        )

        if self._check_source_freshness:
            self._apply_source_freshness(context)

        try:
            use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None
            logger.debug("DbtProducerWatcherOperator: use_events=%s", use_events)

            startup_events: list[dict[str, Any]] = []

            if use_events:

                def _callback(event_message: EventMsg) -> None:
                    try:
                        _process_dbt_log_event(context["ti"], event_message)
                        name = event_message.info.name
                        if name in {"MainReportVersion", "AdapterRegistered"}:
                            self._handle_startup_event(event_message, startup_events)
                            safe_xcom_push(
                                task_instance=context["ti"], key=_DBT_STARTUP_EVENTS_XCOM_KEY, value=startup_events
                            )
                        elif name == "NodeFinished":
                            self._handle_node_finished(event_message, context)
                    except Exception:
                        event_name = getattr(getattr(event_message, "info", None), "name", "unknown")
                        logger.exception(
                            "DbtProducerWatcherOperator: error while handling dbt event '%s'",
                            event_name,
                        )

                self._dbt_runner_callbacks = [_callback]
                result = super().execute(context=context, **kwargs)

                self._finalize(context, startup_events)
                return_value = result
            else:
                # Fallback – push run_results.json via base class helper
                kwargs["push_run_results_to_xcom"] = True
                return_value = super().execute(context=context, **kwargs)

            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            return return_value

        except Exception:
            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            raise


class DbtConsumerWatcherSensor(BaseConsumerSensor, DbtRunLocalOperator):  # type: ignore[misc]
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunLocalOperator.template_fields  # type: ignore[operator]

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
        super().__init__(
            poke_interval=poke_interval,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            producer_task_id=producer_task_id,
            deferrable=deferrable,
            **kwargs,
        )

    def _get_status_from_events(self, ti: Any, context: Context) -> Any:

        self._log_startup_events(ti)

        node_finished_key = f"nodefinished_{self.model_unique_id.replace('.', '__')}"
        logger.info("Pulling from producer task_id: %s, key: %s", self.producer_task_id, node_finished_key)
        compressed_b64_event_msg = ti.xcom_pull(task_ids=self.producer_task_id, key=node_finished_key)

        if not compressed_b64_event_msg:
            return None

        event_json = _parse_compressed_xcom(compressed_b64_event_msg)

        logger.info("Node Info: %s", str(event_json))
        node_result = event_json.get("data", {}).get("run_result", {})
        status = node_result.get("status")
        if status in DBT_FAILED_STATUSES:
            logger.error("%s", node_result.get("message"))

        return status

    def use_event(self) -> bool:
        if not self.invocation_mode:
            self._discover_invocation_mode()
        return self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the producer task.
# That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD, until we have a better experience.
class DbtBuildWatcherOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherOperator(DbtSeedMixin, DbtConsumerWatcherSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherOperator(DbtSnapshotMixin, DbtConsumerWatcherSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherSensor.template_fields


class DbtSourceWatcherOperator(DbtSourceLocalOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.LOCAL.
    """

    template_fields: Sequence[str] = DbtSourceLocalOperator.template_fields  # type: ignore[assignment]


class DbtRunWatcherOperator(DbtConsumerWatcherSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtTestWatcherOperator(EmptyOperator):
    """
    As a starting point, this operator does nothing.
    We'll be implementing this operator as part of: https://github.com/astronomer/astronomer-cosmos/issues/1974
    """

    def __init__(self, *args: Any, **kwargs: Any):
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
