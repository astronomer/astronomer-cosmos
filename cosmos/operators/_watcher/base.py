from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException

from cosmos.config import ProfileConfig
from cosmos.constants import (
    _DBT_STARTUP_EVENTS_XCOM_KEY,
    AIRFLOW_VERSION,
    CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
)
from cosmos.log import get_logger
from cosmos.operators._watcher.aggregation import push_test_result_or_aggregate
from cosmos.operators._watcher.state import (
    DBT_FAILED_STATUSES,
    _iso_to_string,
    _log_dbt_event,
    build_producer_state_fetcher,
    get_xcom_val,
    is_dbt_node_status_success,
    is_dbt_node_status_terminal,
    safe_xcom_push,
)
from cosmos.operators._watcher.triggerer import WatcherTrigger, _parse_compressed_xcom

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator
    from airflow.sdk.definitions.context import Context
except ImportError:  # pragma: no cover
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from dbt_common.events.base_types import EventMsg
except ImportError:  # pragma: no cover
    EventMsg = None

logger = get_logger(__name__)


def _process_dbt_log_event(task_instance: Any, dbt_log: dict[str, Any] | EventMsg) -> None:
    logger.debug("dbt_log: %s", dbt_log)
    sensitive_words = ["fail", "error"]
    if isinstance(dbt_log, dict):  # Subprocess
        data = dbt_log.get("data", {})
        info = dbt_log.get("info", {})

        node_info = data.get("node_info")
        status = node_info.get("node_status") if node_info else None
        unique_id = node_info.get("unique_id") if node_info else None
        start_time = node_info.get("node_started_at") if node_info else None
        finish_time = node_info.get("node_finished_at") if node_info else None
        msg = data.get("msg") or info.get("msg") or None
    else:  # Runner
        node_info = getattr(dbt_log.data, "node_info", None)
        unique_id = getattr(node_info, "unique_id") if node_info else None
        status = getattr(node_info, "node_status", None) if node_info else None
        start_time = getattr(node_info, "node_started_at", None) if node_info else None
        finish_time = getattr(node_info, "node_finished_at", None) if node_info else None
        msg = getattr(dbt_log.info, "msg", None)

    # Special case when node status is the string "None"; only process messages that contain an error or fail word
    if status in ["None"] and msg is not None:
        # Check if there is error log message
        for sensitive_word in sensitive_words:
            if sensitive_word in msg.lower():
                break
        else:
            return None

    if unique_id:
        dbt_event = {
            "status": status,
            "start_time": _iso_to_string(start_time),
            "finish_time": _iso_to_string(finish_time),
            "msg": msg,
        }

        xcom_key = f"{unique_id.replace('.', '__')}_dbt_event"
        # Avoid redundant XCom writes (and global lock contention) by only pushing
        # when the event payload has changed.
        existing_event = get_xcom_val(task_instance=task_instance, key=xcom_key, task_ids=PRODUCER_WATCHER_TASK_ID)
        if existing_event == dbt_event:
            return None
        safe_xcom_push(task_instance=task_instance, key=xcom_key, value=dbt_event)


def _extract_compiled_sql(
    project_dir: str, unique_id: str, node_path: str | None, resource_type: str | None
) -> str | None:
    """
    Extract compiled SQL from the target directory for a given dbt node.

    Used by both the subprocess strategy (via store_dbt_resource_status_from_log)
    and the node-event strategy (via DbtProducerWatcherOperator._handle_node_finished);
    both consume from the same target/compiled layout under project_dir.

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
        key=f"{unique_id.replace('.', '__')}_compiled_sql",
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
    """
    event_name = log_line.get("info", {}).get("name")
    if event_name not in ("MainReportVersion", "AdapterRegistered"):
        return
    info = log_line.get("info", {})
    msg = info.get("msg", "")
    ts = info.get("ts", "")
    current = list(task_instance.xcom_pull(key=_DBT_STARTUP_EVENTS_XCOM_KEY) or [])
    current.append({"name": event_name, "msg": msg, "ts": ts})
    safe_xcom_push(task_instance=task_instance, key=_DBT_STARTUP_EVENTS_XCOM_KEY, value=current)


def store_dbt_resource_status_from_log(
    line: str,
    extra_kwargs: Any,
    *,
    tests_per_model: dict[str, list[str]] | None = None,
    test_results_per_model: dict[str, list[str]] | None = None,
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
        Keyed by model unique_id, values are lists of test statuses (e.g. ``["pass", "pass"]``).
        Mutated in place by this function.
    """
    try:
        log_line = json.loads(line)
        context = extra_kwargs.get("context") if extra_kwargs else None
        ti = context.get("ti") if context else None

        if ti:
            _process_dbt_log_event(ti, log_line)
    except json.JSONDecodeError:
        logger.debug("Failed to parse log: %s", line)
        log_line = {}
    else:
        logger.debug("Log line: %s", log_line)
        context = extra_kwargs.get("context")
        if context is not None:
            _store_startup_event_from_log(context["ti"], log_line)
        node_info = log_line.get("data", {}).get("node_info", {})
        dbt_node_status = node_info.get("node_status")
        dbt_node_resource_type = node_info.get("resource_type")
        unique_id = node_info.get("unique_id")

        logger.debug("Model: %s is in %s state", unique_id, dbt_node_status)

        # Handle terminal statuses for both models (success/failed) and tests (pass/fail)
        # TODO: handle all possible statuses including skipped, warn, etc.
        if is_dbt_node_status_terminal(dbt_node_status):
            context = extra_kwargs.get("context")
            assert context is not None  # Make MyPy happy
            if dbt_node_resource_type == "test" and tests_per_model and test_results_per_model is not None:
                logger.debug("Test '%s' finished with status '%s'", unique_id, dbt_node_status)
                push_test_result_or_aggregate(
                    unique_id, dbt_node_status, tests_per_model, test_results_per_model, context["ti"]
                )
            else:
                safe_xcom_push(
                    task_instance=context["ti"], key=f"{unique_id.replace('.', '__')}_status", value=dbt_node_status
                )

            # Extract and push compiled_sql for models (centralised for both subprocess and node-event)
            # compiled_sql is available for both success and failed models - it's compiled before execution
            project_dir = extra_kwargs.get("project_dir")
            if project_dir:
                store_compiled_sql_for_model(
                    context["ti"], project_dir, unique_id, node_info.get("node_path"), node_info.get("resource_type")
                )

    # Additionally, log the message from dbt logs
    log_info = log_line.get("info", {})
    msg = log_info.get("msg")
    level = log_info.get("level", "INFO").upper()
    ts = log_info.get("ts")
    if msg is not None:
        formatted_ts = _iso_to_string(ts)
        if formatted_ts:
            logger.log(getattr(logging, level, logging.INFO), "%s  %s", formatted_ts, msg)
        else:
            logger.log(getattr(logging, level, logging.INFO), msg)


class BaseConsumerSensor(BaseSensorOperator):  # type: ignore[misc]
    template_fields: tuple[str, ...] = ("model_unique_id", "compiled_sql")  # type: ignore[operator]
    poke_retry_number: int = 0

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
        self.compiled_sql = ""
        kwargs.setdefault("priority_weight", CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WATCHER_TASK_WEIGHT_RULE)

        # We pop extra_context before super().__init__ because BaseSensorOperator does not accept it and
        # would raise on unknown kwargs; we assign after so subclasses and user customisations can use it
        # at runtime (e.g. in execute, poke, or callbacks).
        extra_context = kwargs.pop("extra_context") if "extra_context" in kwargs else {}
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            execution_timeout=execution_timeout,
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

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """
        Handles logic for retrying a failed dbt model execution.
        Reconstructs the dbt command by cloning the project and re-running the model
        with appropriate flags, while ensuring flags like `--select` or `--exclude` are excluded.
        """
        logger.info(
            f"Retry attempt #%s – Running model '%s' from project '%s' using {self.__class__.__name__}",
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

        self.build_and_run_cmd(context, cmd_flags=cmd_flags)  # type: ignore[attr-defined]

        logger.info("dbt run completed successfully on retry for model '%s'", self.model_unique_id)
        return True

    def _get_status_from_run_results(self, ti: Any, context: Context) -> Any:
        compressed_b64_run_results = ti.xcom_pull(task_ids=self.producer_task_id, key="run_results")

        if not compressed_b64_run_results:
            return None

        run_results_json = _parse_compressed_xcom(compressed_b64_run_results)

        logger.debug("Run results: %s", run_results_json)

        results = run_results_json.get("results", [])
        node_result = next((r for r in results if r.get("unique_id") == self.model_unique_id), None)

        if not node_result:  # pragma: no cover
            logger.warning(
                "The dbt node with unique_id '%s' was not executed by the dbt command run in the producer task. This may happen if it is an ephemeral model or if the model sql file is empty.",
                self.model_unique_id,
            )
            return None

        logger.info("Node Info: %s", run_results_json)

        status = node_result.get("status")

        if status in DBT_FAILED_STATUSES:
            logger.error("%s", node_result.get("message"))

        self.compiled_sql = node_result.get("compiled_code")
        if self.compiled_sql and hasattr(self, "_override_rtif"):
            self._override_rtif(context)

        return status

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
                    use_event=self.use_event(),
                    poke_interval=self.poke_interval,
                ),
                timeout=self.execution_timeout,
                method_name=self.execute_complete.__name__,
            )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        status = event.get("status")
        reason = event.get("reason")

        if status == "success" and reason == "model_not_run":
            logger.info(
                "Model '%s' was skipped by the dbt command. This may happen if it is an ephemeral model or if the model sql file is empty.",
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

        dbt_events = get_xcom_val(
            task_instance=context["ti"],
            key=f"{self.model_unique_id.replace('.', '__')}_dbt_event",
            task_ids=self.producer_task_id,
        )
        _log_dbt_event(dbt_events)
        if reason == "model_failed":
            raise AirflowException(
                f"dbt model '{self.model_unique_id}' failed. Review the producer task '{self.producer_task_id}' logs for details."
            )

        if reason == "producer_failed":
            raise AirflowException(
                f"Watcher producer task '{self.producer_task_id}' failed before reporting model results. Check its logs for the underlying error."
            )

    def use_event(self) -> bool:
        raise NotImplementedError("Subclasses must implement this method")

    def _get_status_from_events(self, ti: Any, context: Context) -> Any:
        raise NotImplementedError("Subclasses should implement this method if `use_event` may return True")

    def _log_startup_events(self, ti: Any) -> None:
        dbt_startup_events: list[dict[str, Any]] = ti.xcom_pull(
            task_ids=self.producer_task_id, key=_DBT_STARTUP_EVENTS_XCOM_KEY
        )
        if dbt_startup_events:  # pragma: no cover
            for event in dbt_startup_events:
                # Adding debug level to avoid redundant logs for non-deferrable mode
                logger.debug("%s", event.get("msg"))

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
            return self._fallback_to_non_watcher_run(try_number, context)

        # We have assumption here that both the build producer and the sensor task will have same invocation mode
        producer_task_state = self._get_producer_task_status(context)
        if self.use_event():
            status = self._get_status_from_events(ti, context)
        else:
            self._log_startup_events(ti)
            status = get_xcom_val(ti, self.producer_task_id, f"{self.model_unique_id.replace('.', '__')}_status")

        # compiled_sql is always in the canonical per-model XCom key (same for event and subprocess modes)
        if status is not None:
            compiled_sql = get_xcom_val(
                ti, self.producer_task_id, f"{self.model_unique_id.replace('.', '__')}_compiled_sql"
            )
            if compiled_sql:
                self.compiled_sql = compiled_sql
                if hasattr(self, "_override_rtif"):
                    self._override_rtif(context)

        dbt_events = get_xcom_val(
            task_instance=context["ti"],
            key=f"{self.model_unique_id.replace('.', '__')}_dbt_event",
            task_ids=self.producer_task_id,
        )
        _log_dbt_event(dbt_events)

        if status is None:

            if producer_task_state == "failed":
                if self.poke_retry_number > 0:
                    raise AirflowException(
                        f"The dbt build command failed in producer task. Please check the log of task {self.producer_task_id} for details."
                    )
                else:
                    # This handles the scenario of tasks that failed with `State.UPSTREAM_FAILED`
                    return self._fallback_to_non_watcher_run(try_number, context)

            self.poke_retry_number += 1

            return False
        elif is_dbt_node_status_success(status):
            return True
        else:
            raise AirflowException(f"Model '{self.model_unique_id}' finished with status '{status}'")
