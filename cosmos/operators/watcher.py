from __future__ import annotations

import ast
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

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator
except ImportError:
    from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException

from cosmos.constants import InvocationMode
from cosmos.operators.local import DbtLocalBaseOperator

try:
    from dbt_common.events.base_types import EventMsg
except ImportError:  # pragma: no cover
    EventMsg = None

logger = logging.getLogger(__name__)


PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT = 9999

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


class DbtModelStatusSensor(BaseSensorOperator, DbtRunLocalOperator):
  template_fields = ("model_unique_id",)

    def __init__(
        self,
        *,
        model_unique_id: str,
        profile_config: ProfileConfig | None = None,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        master_task_id: str = "dbt_build_coordinator",
        poke_interval: int = 20,
        timeout: int = 60 * 60,  # 1 h safety valve # TODO: Test for custom value
        **kwargs: Any,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            **kwargs,
        )
        self.model_unique_id = model_unique_id
        self.master_task_id = master_task_id

    @staticmethod
    def _filter_flags(flags: list[str]) -> list[str]:
        """Filters out dbt flags that are incompatible with retry (e.g., --select, --exclude)."""
        filtered = []
        skip_next = False
        for token in flags:
            if skip_next:
                if token.startswith("--"):
                    skip_next = False
                else:
                    continue  # skip value of previous flag
            if token in ("--select", "--exclude"):
                skip_next = True
                continue
            filtered.append(token)
        return filtered

    def _handle_task_retry(self, try_number: int, context: Context) -> bool:
        """
        Handles logic for retrying a failed dbt model execution.
        Reconstructs the dbt command by cloning the project and re-running the model
        with appropriate flags, while ensuring flags like `--select` or `--exclude` are excluded.
        """
        self.log.info(
            "Retry attempt #%s – Re-running model '%s' from project '%s'",
            try_number - 1,
            self.model_unique_id,
            self.project_dir,
        )

        upstream_task = context["ti"].task.dag.get_task(self.master_task_id)

        extra_flags: list[str] = []
        if upstream_task and hasattr(upstream_task, "add_cmd_flags"):
            raw_flags = upstream_task.add_cmd_flags()
            extra_flags = self._filter_flags(raw_flags)

        model_selector = self.model_unique_id.split(".")[-1]
        cmd_flags = extra_flags + ["--select", model_selector]

        self.build_and_run_cmd(context, cmd_flags=cmd_flags)

        self.log.info("dbt run completed successfully on retry for model '%s'", self.model_unique_id)
        return True

    def poke(self, context: Context) -> bool:
        """
        Checks the status of a dbt model run by pulling relevant XComs from the master task.
        Handles retries and checks for successful completion of the model execution.
        """
        try_number = context["ti"].try_number
        ti = context["ti"]

        if try_number > 1:
            return self._handle_task_retry(try_number, context)

        logger.info(
            "Pulling status from task_id %s for model %s",
            self.master_task_id,
            self.model_unique_id,
        )

        dbt_startup_events = ti.xcom_pull(task_ids=self.master_task_id, key="dbt_startup_events")
        if dbt_startup_events:
            self.log.info("dbt_startup_events: %s", dbt_startup_events)  # TODO: FixMe

        pipeline_outlets = ti.xcom_pull(task_ids=self.master_task_id, key="pipeline_outlets")
        if pipeline_outlets:
            self.log.info("pipeline_outlets: %s", pipeline_outlets)  # TODO: FixMe

        node_finished_key = f"nodefinished_{self.model_unique_id.replace('.', '__')}"
        compressed_b64_event_data = ti.xcom_pull(task_ids=self.master_task_id, key=node_finished_key)

        if not compressed_b64_event_data:
            return False

        compressed_event_msg = base64.b64decode(compressed_b64_event_data)
        event_msg = zlib.decompress(compressed_event_msg).decode("utf-8")
        if event_msg:
            self.log.info("event_data: %s", event_msg)  # TODO: FixMe

        event_json = ast.literal_eval(event_msg)
        status = event_json.get("data", {}).get("run_result", {}).get("status")

        self.log.info("Model %s finished with status %s", self.model_unique_id, status)

        if status == "success":
            return True
        else:
            raise AirflowException(f"Model {self.model_unique_id} finished with status '{status}'")
