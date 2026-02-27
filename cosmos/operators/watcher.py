from __future__ import annotations

import base64
import json
import zlib
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

from cosmos.config import ProfileConfig
from cosmos.operators._watcher import _parse_compressed_xcom, safe_xcom_push
from cosmos.settings import watcher_dbt_execution_queue

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.constants import (
    PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
    InvocationMode,
)
from cosmos.log import get_logger
from cosmos.operators._watcher.base import (
    BaseConsumerSensor,
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
    """

    template_fields = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]
    # Use staticmethod to prevent Python's descriptor protocol from binding the function to `self`
    # when accessed via instance, which would incorrectly pass `self` as the first argument
    _process_log_line_callable: Callable[[str, Any], None] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
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
        payload = base64.b64encode(zlib.compress(json.dumps(event_message_dict).encode())).decode()
        safe_xcom_push(task_instance=context["ti"], key=f"nodefinished_{uid.replace('.', '__')}", value=payload)

    def _finalize(self, context: Context, startup_events: list[dict[str, Any]]) -> None:
        # Only push startup events; per-model statuses are available via individual nodefinished_<uid> entries.
        if startup_events:
            safe_xcom_push(task_instance=context["ti"], key="dbt_startup_events", value=startup_events)

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
            self._process_log_line_callable = store_dbt_resource_status_from_log

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

        try:
            use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None
            logger.debug("DbtProducerWatcherOperator: use_events=%s", use_events)

            startup_events: list[dict[str, Any]] = []

            if use_events:

                def _callback(event_message: EventMsg) -> None:
                    try:
                        name = event_message.info.name
                        if name in {"MainReportVersion", "AdapterRegistered"}:
                            self._handle_startup_event(event_message, startup_events)
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
        timeout: int = 60 * 60,  # 1 h safety valve
        execution_timeout: timedelta = timedelta(hours=1),
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            execution_timeout=execution_timeout,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            producer_task_id=producer_task_id,
            deferrable=deferrable,
            **kwargs,
        )

    def _get_status_from_events(self, ti: Any, context: Context) -> Any:
        dbt_startup_events = ti.xcom_pull(task_ids=self.producer_task_id, key="dbt_startup_events")
        if dbt_startup_events:  # pragma: no cover
            logger.info("Dbt Startup Event: %s", dbt_startup_events)

        node_finished_key = f"nodefinished_{self.model_unique_id.replace('.', '__')}"
        logger.info("Pulling from producer task_id: %s, key: %s", self.producer_task_id, node_finished_key)
        compressed_b64_event_msg = ti.xcom_pull(task_ids=self.producer_task_id, key=node_finished_key)

        if not compressed_b64_event_msg:
            return None

        event_json = _parse_compressed_xcom(compressed_b64_event_msg)

        logger.info("Node Info: %s", event_json)

        return event_json.get("data", {}).get("run_result", {}).get("status")

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
