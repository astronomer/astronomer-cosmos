import json
from datetime import timedelta
from typing import Any

from airflow.exceptions import AirflowException

from cosmos.config import ProfileConfig
from cosmos.constants import (
    AIRFLOW_VERSION,
    CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
)
from cosmos.log import get_logger
from cosmos.operators._watcher.state import build_producer_state_fetcher, get_xcom_val, safe_xcom_push
from cosmos.operators._watcher.triggerer import WatcherTrigger, _parse_compressed_xcom

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator
    from airflow.sdk.definitions.context import Context
except ImportError:  # pragma: no cover
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.context import Context  # type: ignore[attr-defined]


logger = get_logger(__name__)



def store_dbt_resource_status_from_log(line: str, extra_kwargs: Any) -> None:
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
        extra_context = kwargs.pop("extra_context") if "extra_context" in kwargs else {}
        kwargs.setdefault("priority_weight", CONSUMER_WATCHER_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WATCHER_TASK_WEIGHT_RULE)
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            execution_timeout=execution_timeout,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            **kwargs,
        )
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
            logger.warning("No matching result found for unique_id '%s'", self.model_unique_id)
            return None

        logger.info("Node Info: %s", run_results_json)
        self.compiled_sql = node_result.get("compiled_code")
        if self.compiled_sql:
            self._override_rtif(context)  # type: ignore[attr-defined]

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
                    use_event=self.use_event(),
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

    def use_event(self) -> bool:
        raise NotImplementedError("Subclasses must implement this method")

    def _get_status_from_events(self, ti: Any, context: Context) -> Any:
        raise NotImplementedError("Subclasses should implement this method if `use_event` may return True")

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
            status = get_xcom_val(ti, self.producer_task_id, f"{self.model_unique_id.replace('.', '__')}_status")

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
        elif status == "success":
            return True
        else:
            raise AirflowException(f"Model '{self.model_unique_id}' finished with status '{status}'")
