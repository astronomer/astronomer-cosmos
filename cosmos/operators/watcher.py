from __future__ import annotations

import base64
import json
import logging
import zlib
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosmos._triggers.watcher import WatcherTrigger, _parse_compressed_xcom
from cosmos.operators._watcher.state import get_xcom_val, safe_xcom_push

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator
except ImportError:  # pragma: no cover
    from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]


from cosmos.config import ProfileConfig
from cosmos.constants import AIRFLOW_VERSION, PRODUCER_WATCHER_TASK_ID, InvocationMode
from cosmos.operators._watcher.state import build_producer_state_fetcher
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

logger = logging.getLogger(__name__)

CONSUMER_OPERATOR_DEFAULT_PRIORITY_WEIGHT = 10
PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT = 9999
WEIGHT_RULE = "absolute"  # the default "downstream" does not work with dag.test()


def _store_dbt_resource_status_from_log(line: str, extra_kwargs: Any) -> None:
    """
    Parses a single line from dbt JSON logs and stores node status to Airflow XCom.

    This method parses each log line from dbt when --log-format json is used,
    extracts node status information, and pushes it to XCom for consumption
    by downstream watcher sensors.
    """
    try:
        log_line = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("Failed to parse log: %s", line)
        log_line = {}
    node_info = log_line.get("data", {}).get("node_info", {})
    node_status = node_info.get("node_status")
    unique_id = node_info.get("unique_id")

    logger.debug("Model: %s is in %s state", unique_id, node_status)

    # TODO: Handle and store all possible node statuses, not just the current success and failed
    if node_status in ["success", "failed"]:
        context = extra_kwargs.get("context")
        assert context is not None  # Make MyPy happy
        safe_xcom_push(task_instance=context["ti"], key=f"{unique_id.replace('.', '__')}_status", value=node_status)


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
    _process_log_line_callable = staticmethod(_store_dbt_resource_status_from_log)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")
        kwargs.setdefault("priority_weight", PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WEIGHT_RULE)
        # Consumer watcher retry logic handles model-level reruns using the LOCAL execution mode; rerunning the producer
        # would repeat the full dbt build and duplicate watcher callbacks which may not be processed by the consumers if
        # they have already processed output XCOMs from the first run of the producer, so we disable retries.
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0
        kwargs["log_format"] = "json"

        super().__init__(task_id=task_id, *args, **kwargs)

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

    def _extract_compiled_sql_for_node_event(self, event_message: EventMsg) -> str | None:
        if getattr(event_message.data.node_info, "resource_type", None) != "model":
            return None
        uid = event_message.data.node_info.unique_id
        node_path = str(event_message.data.node_info.node_path)
        package = uid.split(".")[1]
        compiled_sql_path = Path.cwd() / "target" / "compiled" / package / "models" / node_path
        if not compiled_sql_path.exists():
            logger.warning(
                "Compiled sql path %s does not exist and hence the rendered template field compiled_sql for the model will not be populated",
                compiled_sql_path,
            )
            return None
        return compiled_sql_path.read_text(encoding="utf-8").strip() or None

    def _handle_node_finished(
        self,
        event_message: EventMsg,
        context: Context,
    ) -> None:
        logger.debug("DbtProducerWatcherOperator: handling node finished event: %s", event_message)
        uid = event_message.data.node_info.unique_id
        event_message_dict = self._serialize_event(event_message)
        compiled_sql = self._extract_compiled_sql_for_node_event(event_message)
        if compiled_sql:
            event_message_dict["compiled_sql"] = compiled_sql
        payload = base64.b64encode(zlib.compress(json.dumps(event_message_dict).encode())).decode()
        safe_xcom_push(task_instance=context["ti"], key=f"nodefinished_{uid.replace('.', '__')}", value=payload)

    def _finalize(self, context: Context, startup_events: list[dict[str, Any]]) -> None:
        # Only push startup events; per-model statuses are available via individual nodefinished_<uid> entries.
        if startup_events:
            safe_xcom_push(task_instance=context["ti"], key="dbt_startup_events", value=startup_events)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("DbtProducerWatcherOperator expects a task instance in the execution context")

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            retry_message = (
                "Dbt WATCHER producer task does not support Airflow retries. "
                f"Detected attempt #{try_number}; failing fast to avoid running a second dbt build."
            )
            self.log.error(retry_message)
            raise AirflowException(retry_message)

        self.log.info(
            "Dbt WATCHER producer task forces Airflow retries to 0 so the dbt build only runs once; "
            "downstream sensors own model-level retries."
        )

        try:
            if not self.invocation_mode:
                self._discover_invocation_mode()

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


class DbtConsumerWatcherSensor(BaseSensorOperator, DbtRunLocalOperator):  # type: ignore[misc]
    template_fields: tuple[str, ...] = DbtRunLocalOperator.template_fields + ("model_unique_id",)  # type: ignore[operator]
    poke_retry_number: int = 0

    def __init__(
        self,
        *,
        profile_config: ProfileConfig | None = None,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        producer_task_id: str = PRODUCER_WATCHER_TASK_ID,
        poke_interval: int = 10,
        timeout: int = 60 * 60,  # 1 h safety valve
        execution_timeout: timedelta = timedelta(hours=1),
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        self.compiled_sql = ""
        extra_context = kwargs.pop("extra_context") if "extra_context" in kwargs else {}
        kwargs.setdefault("priority_weight", CONSUMER_OPERATOR_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WEIGHT_RULE)
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            execution_timeout=execution_timeout,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            **kwargs,
        )
        self.model_unique_id = extra_context.get("dbt_node_config", {}).get("unique_id")
        self.producer_task_id = producer_task_id
        self.deferrable = deferrable

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

    def _fallback_to_local_run(self, try_number: int, context: Context) -> bool:
        """
        Handles logic for retrying a failed dbt model execution.
        Reconstructs the dbt command by cloning the project and re-running the model
        with appropriate flags, while ensuring flags like `--select` or `--exclude` are excluded.
        """
        logger.info(
            "Retry attempt #%s – Running model '%s' from project '%s' using ExecutionMode.LOCAL",
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

        logger.info("dbt run completed successfully on retry for model '%s'", self.model_unique_id)
        return True

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

        self.compiled_sql = event_json.get("compiled_sql", "")
        if self.compiled_sql:
            self._override_rtif(context)

        return event_json.get("data", {}).get("run_result", {}).get("status")

    def _get_status_from_run_results(self, ti: Any, context: Context) -> Any:
        compressed_b64_run_results = ti.xcom_pull(task_ids=self.producer_task_id, key="run_results")

        if not compressed_b64_run_results:
            return None

        run_results_json = _parse_compressed_xcom(compressed_b64_run_results)

        logger.debug("Run results: %s", run_results_json)

        results = run_results_json.get("results", [])
        node_result = next((r for r in results if r.get("unique_id") == self.model_unique_id), None)

        if not node_result:  # pragma: no cover
            logger.warning("No matching result found for unique_id '%s'", self.model_unique_id)
            return None

        logger.info("Node Info: %s", run_results_json)
        self.compiled_sql = node_result.get("compiled_code")
        if self.compiled_sql:
            self._override_rtif(context)

        return node_result.get("status")

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

    def execute(self, context: Context, **kwargs: Any) -> None:
        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context):
            self.defer(
                trigger=WatcherTrigger(
                    model_unique_id=self.model_unique_id,
                    producer_task_id=self.producer_task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    map_index=context["task_instance"].map_index,
                    use_event=self._use_event(),
                    poke_interval=self.poke_interval,
                ),
                timeout=self.execution_timeout,
                method_name=self.execute_complete.__name__,
            )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        status = event.get("status")
        if status != "failed":
            return

        reason = event.get("reason")
        if reason == "model_failed":
            raise AirflowException(
                f"dbt model '{self.model_unique_id}' failed. Review the producer task '{self.producer_task_id}' logs for details."
            )

        if reason == "producer_failed":
            raise AirflowException(
                f"Watcher producer task '{self.producer_task_id}' failed before reporting model results. Check its logs for the underlying error."
            )

    def _use_event(self) -> bool:
        if not self.invocation_mode:
            self._discover_invocation_mode()
        return self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None

    def poke(self, context: Context) -> bool:
        """
        Checks the status of a dbt model run by pulling relevant XComs from the master task.
        Handles retries and checks for successful completion of the model execution.
        """
        ti = context["ti"]
        try_number = ti.try_number

        logger.info(
            "Try number #%s, poke attempt #%s: Pulling status from task_id '%s' for model '%s'",
            try_number,
            self.poke_retry_number,
            self.producer_task_id,
            self.model_unique_id,
        )

        if try_number > 1:
            return self._fallback_to_local_run(try_number, context)

        # We have assumption here that both the build producer and the sensor task will have same invocation mode
        producer_task_state = self._get_producer_task_status(context)
        if self._use_event():
            status = self._get_status_from_events(ti, context)
        else:
            status = get_xcom_val(ti, self.producer_task_id, f"{self.model_unique_id.replace('.', '__')}_status")

        if status is None:

            if producer_task_state == "failed":
                if self.poke_retry_number > 0:
                    raise AirflowException(
                        f"The dbt build command failed in producer task. Please check the log of task {self.producer_task_id} for details."
                    )
                else:
                    # This handles the scenario of tasks that failed with `State.UPSTREAM_FAILED`
                    return self._fallback_to_local_run(try_number, context)

            self.poke_retry_number += 1

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
