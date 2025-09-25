from __future__ import annotations

import ast
import base64
import json
import logging
import zlib
from typing import TYPE_CHECKING, Any, Sequence

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

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.config import ProfileConfig
from cosmos.constants import PRODUCER_WATCHER_TASK_ID, InvocationMode
from cosmos.operators.base import (
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


class DbtConsumerWatcherSensor(BaseSensorOperator, DbtRunLocalOperator):  # type: ignore[misc]
    template_fields = ("model_unique_id",)

    def __init__(
        self,
        *,
        profile_config: ProfileConfig | None = None,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        producer_task_id: str = "dbt_producer_watcher",
        poke_interval: int = 20,
        timeout: int = 60 * 60,  # 1 h safety valve
        **kwargs: Any,
    ) -> None:
        extra_context = kwargs.pop("extra_context") if "extra_context" in kwargs else {}
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            **kwargs,
        )
        self.model_unique_id = extra_context.get("dbt_node_config", {}).get("unique_id")
        self.producer_task_id = producer_task_id

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

        upstream_task = context["ti"].task.dag.get_task(self.producer_task_id)

        extra_flags: list[str] = []
        if upstream_task and hasattr(upstream_task, "add_cmd_flags"):
            raw_flags = upstream_task.add_cmd_flags()
            extra_flags = self._filter_flags(raw_flags)

        model_selector = self.model_unique_id.split(".")[-1]
        cmd_flags = extra_flags + ["--select", model_selector]

        self.build_and_run_cmd(context, cmd_flags=cmd_flags)

        self.log.info("dbt run completed successfully on retry for model '%s'", self.model_unique_id)
        return True

    def _get_status_from_events(self, ti: Any) -> Any:

        dbt_startup_events = ti.xcom_pull(task_ids=self.producer_task_id, key="dbt_startup_events")
        if dbt_startup_events:  # pragma: no cover
            self.log.info("Dbt Startup Event: %s", dbt_startup_events)

        node_finished_key = f"nodefinished_{self.model_unique_id.replace('.', '__')}"
        compressed_b64_event_msg = ti.xcom_pull(task_ids=self.producer_task_id, key=node_finished_key)

        if not compressed_b64_event_msg:
            return None

        compressed_bytes = base64.b64decode(compressed_b64_event_msg)
        event_json_str = zlib.decompress(compressed_bytes).decode("utf-8")
        event_json = json.loads(event_json_str)

        self.log.info("Node Info: %s", event_json_str)

        return event_json.get("data", {}).get("run_result", {}).get("status")

    def _get_status_from_run_results(self, ti: Any) -> Any:
        compressed_b64_run_results = ti.xcom_pull(task_ids=self.producer_task_id, key="run_results")

        if not compressed_b64_run_results:
            return None

        compressed_bytes = base64.b64decode(compressed_b64_run_results)
        run_results_str = zlib.decompress(compressed_bytes).decode("utf-8")
        run_results_json = json.loads(run_results_str)

        self.log.debug("Run results: %s", run_results_json)

        results = run_results_json.get("results", [])
        node_result = next((r for r in results if r.get("unique_id") == self.model_unique_id), None)

        if not node_result:  # pragma: no cover
            self.log.warning("No matching result found for unique_id '%s'", self.model_unique_id)
            return None

        self.log.info("Node Info: %s", run_results_str)
        return node_result.get("status")

    def poke(self, context: Context) -> bool:
        """
        Checks the status of a dbt model run by pulling relevant XComs from the master task.
        Handles retries and checks for successful completion of the model execution.
        """
        ti = context["ti"]
        try_number = ti.try_number

        if try_number > 1:
            return self._handle_task_retry(try_number, context)

        self.log.info(
            "Pulling status from task_id '%s' for model '%s'",
            self.producer_task_id,
            self.model_unique_id,
        )

        # We have assumption here that both the build producer and the sensor task will have same invocation mode
        if not self.invocation_mode:
            self._discover_invocation_mode()
        use_events = self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None

        if use_events:
            status = self._get_status_from_events(ti)
        else:
            status = self._get_status_from_run_results(ti)

        if status is None:
            return False
        elif status == "success":
            return True
        else:
            raise AirflowException(f"Model '{self.model_unique_id}' finished with status '{status}'")


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the producer task.
# That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD, until we have a better experience.
class DbtBuildWatcherOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherOperator(DbtSeedMixin, DbtModelStatusSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str] = DbtModelStatusSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherOperator(DbtSnapshotMixin, DbtModelStatusSensor):
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str] = DbtModelStatusSensor.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSourceWatcherOperator(DbtSourceLocalOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.LOCAL.
    """

    template_fields: Sequence[str] = DbtSourceLocalOperator.template_fields

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtRunWatcherOperator(DbtModelStatusSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str] = DbtModelStatusSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

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
