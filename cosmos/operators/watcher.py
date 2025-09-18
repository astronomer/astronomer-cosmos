from __future__ import annotations

import base64
import gzip
import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.config import RenderConfig
from cosmos.constants import InvocationMode
from cosmos.operators.local import DbtBuildLocalOperator

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


class DbtBuildCoordinatorOperator(DbtBuildLocalOperator):
    """Run dbt build and coordinate model run statuses via XCom for *WATCHER* execution mode .

    Executes **one** ``dbt build`` covering the whole selection.

    - **When ``InvocationMode.DBT_RUNNER`` is set** we patch
      ``dbtRunner`` so we receive structured events *while* dbt is running.  In
      this real-time mode the operator:
        – pushes startup metadata events (``MainReportVersion``,
          ``AdapterRegistered``) together under XCom key
          ``dbt_startup_events``;
        – pushes each ``NodeFinished`` event immediately to XCom under
          ``nodefinished_<unique_id>`` (gzipped+base64 JSON) so downstream
          sensors can react with near-zero latency.

    - **When ``dbtRunner`` is *not* available** (older dbt or
      ``InvocationMode=SUBPROCESS``) we fallback to delayed strategy: after
      dbt exits we read ``target/run_results.json`` and push the whole mapping
      once under key ``run_results`` to XCom.  Sensors can poll this key but will not
      get per-model updates until the build completes.

    This keeps the heavy dbt work centralised while providing near real-time
    feedback and granular task-level observability downstream.
    """

    def __init__(
        self,
        *,
        render_config: RenderConfig | None = None,
        **kwargs: Any,
    ) -> None:
        # Store so we can honour select/exclude when building flags
        self.render_config: RenderConfig | None = render_config

        task_id = kwargs.pop("task_id", "dbt_build_coordinator")
        super().__init__(task_id=task_id, **kwargs)

    def add_cmd_flags(self) -> list[str]:
        flags: list[str] = super().add_cmd_flags()

        self.log.info("DbtBuildCoordinatorOperator: render_config: %s", self.render_config)
        if self.render_config is not None and self.render_config.exclude:
            flags.extend(["--exclude", *self.render_config.exclude])
        if self.render_config is not None and self.render_config.select:
            flags.extend(["--select", *self.render_config.select])
        return flags

    def execute(self, context: Context, **kwargs: Any) -> Any:  # type: ignore[override]
        if not self.invocation_mode:
            self._discover_invocation_mode()

        # Prefer structured events for low-latency status streaming when the dbtRunner is available.
        use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None
        self.log.debug("DbtBuildCoordinatorOperator: use_events: %s", use_events)

        results_mapping: dict[str, str] = {}
        startup_events: list[dict[str, Any]] = []  # capture metadata events

        if use_events:
            logger.info("DbtBuildCoordinatorOperator: capturing node statuses via dbtRunner callbacks")

            ti = context["ti"]

            def _event_callback(ev: EventMsg) -> None:  # type: ignore[valid-type]
                # Capture node completion events
                ev_name = ev.info.name

                if ev_name in {"MainReportVersion", "AdapterRegistered"}:
                    info = ev.info  # type: ignore[attr-defined]
                    raw_ts = getattr(info, "ts", None)
                    if raw_ts is not None and hasattr(raw_ts, "ToJsonString"):
                        ts_val = raw_ts.ToJsonString()
                    else:
                        ts_val = str(raw_ts)
                    startup_events.append(
                        {
                            "name": info.name,
                            "msg": info.msg,
                            "ts": ts_val,
                        }
                    )

                if ev_name == "NodeFinished":
                    from google.protobuf.json_format import MessageToDict

                    uid = ev.data.node_info.unique_id
                    status = ev.data.run_result.status.upper()
                    results_mapping[uid] = status
                    ev_dict = MessageToDict(ev, preserving_proto_field_name=True)
                    payload = base64.b64encode(gzip.compress(json.dumps(ev_dict).encode())).decode()

                    ti.xcom_push(key=f"nodefinished_{uid.replace('.', '__')}", value=payload)

            import cosmos.dbt.runner as _dbt_runner_mod

            original_get_runner = _dbt_runner_mod.get_runner

            def _patched_get_runner() -> Any:
                from dbt.cli.main import dbtRunner

                return dbtRunner(callbacks=[_event_callback])

            # Monkey-patch get_runner so AbstractDbtLocalBase picks up patched runner with event callbacks.
            _dbt_runner_mod.get_runner = _patched_get_runner  # type: ignore[assignment]
            if hasattr(original_get_runner, "cache_clear"):
                original_get_runner.cache_clear()

            try:
                result = super().execute(context=context, **kwargs)
            finally:
                _dbt_runner_mod.get_runner = original_get_runner

            logger.info("Captured %d node statuses from dbtRunner events", len(results_mapping))

            self.log.debug("Startup events: %s", startup_events)
            if startup_events:
                ti.xcom_push(key="dbt_startup_events", value=startup_events)
        else:
            logger.info("DbtBuildCoordinatorOperator: falling back to run_results.json for status capture")
            kwargs["push_run_results_to_xcom"] = True
            result = super().execute(context=context, **kwargs)

        return result
