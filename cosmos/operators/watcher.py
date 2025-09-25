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


# Example dbt event JSON dictionaries (kept for reference)
nodefinished_model__fhir_dbt_utils__fhir_table_list = {
    "info": {
        "name": "NodeFinished",
        "code": "Q025",
        "msg": "Finished running node model.fhir_dbt_utils.fhir_table_list",
        "level": "debug",
        "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
        "pid": 34,
        "thread": "Thread-4 (worker)",
        "ts": {"seconds": 1757613874, "nanos": 97893000},
    },
    "data": {
        "node_info": {
            "node_path": "fhir_resources/fhir_table_list.sql",
            "node_name": "fhir_table_list",
            "unique_id": "model.fhir_dbt_utils.fhir_table_list",
            "resource_type": "model",
            "materialized": "table",
            "node_status": "success",
            "node_started_at": "2025-09-11T18:04:30.755110",
            "node_finished_at": "2025-09-11T18:04:34.094207",
            "meta": {"fields": {"description": "List of FHIR resource tables present in the database"}},
            "node_relation": {
                "database": "astronomer-dag-authoring",
                "schema": "fhir_airflow3",
                "alias": "fhir_table_list",
                "relation_name": "`astronomer-dag-authoring`.`fhir_airflow3`.`fhir_table_list`",
            },
        },
        "run_result": {
            "status": "success",
            "message": "CREATE TABLE (17.0 rows, 10.0 MiB processed)",
            "timing_info": [
                {
                    "name": "compile",
                    "started_at": {"seconds": 1757613870, "nanos": 796102000},
                    "completed_at": {"seconds": 1757613870, "nanos": 848620000},
                },
                {
                    "name": "execute",
                    "started_at": {"seconds": 1757613870, "nanos": 848916000},
                    "completed_at": {"seconds": 1757613874, "nanos": 91289000},
                },
            ],
            "thread": "Thread-4 (worker)",
            "execution_time": 3.32948399,
            "adapter_response": {
                "slot_ms": 6785,
                "rows_affected": 17,
                "project_id": "astronomer-dag-authoring",
                "location": "US",
                "job_id": "a63ef0bf-d3dc-4146-af3a-a03802e7e493",
                "code": "CREATE TABLE",
                "bytes_processed": 10485760,
                "bytes_billed": 10485760,
                "_message": "CREATE TABLE (17.0 rows, 10.0 MiB processed)",
            },
        },
    },
}

dbt_watcher_mainreportversion = {
    "event": {
        "info": {
            "name": "MainReportVersion",
            "code": "A001",
            "msg": "Running with dbt=1.9.0",
            "level": "info",
            "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
            "pid": 34,
            "thread": "MainThread",
            "ts": {"seconds": 1757613865, "nanos": 813262000},
        },
        "data": {"version": "=1.9.0", "log_version": 3},
    }
}

dbt_watcher_adapterregistered = {
    "event": {
        "info": {
            "name": "AdapterRegistered",
            "code": "E034",
            "msg": "Registered adapter: bigquery=1.9.0",
            "level": "info",
            "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
            "pid": 34,
            "thread": "MainThread",
            "ts": {"seconds": 1757613866, "nanos": 467522000},
        },
        "data": {"adapter_name": "bigquery", "adapter_version": "=1.9.0"},
    }
}


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
