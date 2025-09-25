from __future__ import annotations

import base64
import json
import logging
import zlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.constants import InvocationMode
from cosmos.operators.local import DbtLocalBaseOperator

try:
    from dbt_common.events.base_types import EventMsg
except ImportError:  # pragma: no cover
    EventMsg = None

logger = logging.getLogger(__name__)


PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT = 9999


class DbtProducerWatcherOperator(DbtLocalBaseOperator):
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

    base_cmd = ["build"]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")
        kwargs["priority_weight"] = kwargs.get("priority_weight", PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT)
        super().__init__(task_id=task_id, *args, **kwargs)

    @staticmethod
    def _serialize_event(ev: EventMsg) -> dict[str, Any]:
        """Convert structured dbt EventMsg to plain dict."""
        from google.protobuf.json_format import MessageToDict

        return MessageToDict(ev, preserving_proto_field_name=True)  # type: ignore[no-any-return]

    def _handle_startup_event(self, ev: EventMsg, startup_events: list[dict[str, Any]]) -> None:
        info = ev.info  # type: ignore[attr-defined]
        raw_ts = getattr(info, "ts", None)
        ts_val = raw_ts.ToJsonString() if hasattr(raw_ts, "ToJsonString") else str(raw_ts)  # type: ignore[union-attr]
        startup_events.append({"name": info.name, "msg": info.msg, "ts": ts_val})

    def _handle_node_finished(
        self,
        ev: EventMsg,
        context: Context,
    ) -> None:
        self.log.debug("DbtProducerWatcherOperator: handling node finished event: %s", ev)
        ti = context["ti"]
        uid = ev.data.node_info.unique_id
        ev_dict = self._serialize_event(ev)
        payload = base64.b64encode(zlib.compress(json.dumps(ev_dict).encode())).decode()
        ti.xcom_push(key=f"nodefinished_{uid.replace('.', '__')}", value=payload)

    def _finalize(self, context: Context, startup_events: list[dict[str, Any]]) -> None:
        ti = context["ti"]
        # Only push startup events; per-model statuses are available via individual nodefinished_<uid> entries.
        if startup_events:
            ti.xcom_push(key="dbt_startup_events", value=startup_events)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        if not self.invocation_mode:
            self._discover_invocation_mode()

        use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None
        self.log.debug("DbtProducerWatcherOperator: use_events=%s", use_events)

        startup_events: list[dict[str, Any]] = []

        if use_events:

            def _callback(ev: EventMsg) -> None:
                name = ev.info.name
                if name in {"MainReportVersion", "AdapterRegistered"}:
                    self._handle_startup_event(ev, startup_events)
                elif name == "NodeFinished":
                    self._handle_node_finished(ev, context)

            self._dbt_runner_callbacks = [_callback]
            result = super().execute(context=context, **kwargs)

            self._finalize(context, startup_events)
            return result

        # Fallback – push run_results.json via base class helper
        kwargs["push_run_results_to_xcom"] = True
        return super().execute(context=context, **kwargs)
