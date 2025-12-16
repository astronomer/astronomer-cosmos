from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

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
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback, client_type

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.airflow._override import CosmosKubernetesPodManager
from cosmos.config import ProfileConfig
from cosmos.constants import AIRFLOW_VERSION, PRODUCER_WATCHER_TASK_ID
from cosmos.operators._watcher.state import build_producer_state_fetcher, get_xcom_val, safe_xcom_push
from cosmos.operators._watcher.triggerers import WatcherTrigger
from cosmos.operators.base import (
    DbtRunMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
)
from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSourceKubernetesOperator,
)

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
        logger.info("Failed to parse log: %s", line)
        log_line = {}
    node_info = log_line.get("data", {}).get("node_info", {})
    node_status = node_info.get("node_status")
    unique_id = node_info.get("unique_id")
    if unique_id is not None:
        logger.info("Model: %s is in %s state", unique_id, node_status)

        # TODO: Handle and store all possible node statuses, not just the current success and failed
        if node_status in ["success", "failed"]:
            # These lines raise an exception when using KubernetesPodOperatorCallback
            context = extra_kwargs.get("context")
            assert context is not None  # Make MyPy happy
            safe_xcom_push(task_instance=context["ti"], key=f"{unique_id.replace('.', '__')}_status", value=node_status)


producer_task_context = None


class WatcherKubernetesCallback(KubernetesPodOperatorCallback):  # type: ignore[misc]

    @staticmethod
    def progress_callback(*, line: str, client: client_type, mode: str, **kwargs: Any) -> None:
        line_content = line.strip()
        logger.info(f"[progress_callback] {line_content}")
        try:
            k8s_timestamp, dbt_log = line_content.split(" ", 1)
        except ValueError:
            logger.debug(f"[XUBIRU] Failed to parse log: {line_content}")
        else:
            if not "context" in kwargs:
                kwargs["context"] = producer_task_context
            _store_dbt_resource_status_from_log(dbt_log, kwargs)


class DbtProducerKubernetesWatcherOperator(DbtBuildKubernetesOperator):

    template_fields: tuple[str, ...] = tuple(DbtBuildKubernetesOperator.template_fields) + ("deferrable",)
    _process_log_line_callable: Callable[[str, dict[str, Any]], None] | None = _store_dbt_resource_status_from_log

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")

        super().__init__(task_id=task_id, *args, callbacks=WatcherKubernetesCallback, **kwargs)
        self.dbt_cmd_flags += ["--log-format", "json"]

    @cached_property
    def pod_manager(self) -> CosmosKubernetesPodManager:
        return CosmosKubernetesPodManager(client=self.client, callbacks=self.callbacks)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        global producer_task_context
        producer_task_context = context
        return super().execute(context, **kwargs)


class DbtConsumerKubernetesWatcherSensor(BaseSensorOperator, DbtRunKubernetesOperator):
    template_fields: tuple[str, ...] = ("model_unique_id", "compiled_sql")  # type: ignore[operator]
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

    # minor change from original DbtConsumerWatcherSensor to use K8s run
    def _fallback_to_k8s_run(self, try_number: int, context: Context) -> bool:
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

    # slightly modified from the original DbtConsumerWatcherSensor to check try run and use K8s run
    def execute(self, context: Context, **kwargs: Any) -> Any:
        ti = context["ti"]
        try_number = ti.try_number
        if try_number > 1:
            return self._fallback_to_k8s_run(try_number, context)
        elif not self.poke(context):
            self.defer(
                trigger=WatcherTrigger(
                    model_unique_id=self.model_unique_id,
                    producer_task_id=self.producer_task_id,
                    dag_id=self.dag_id,
                    run_id=context["run_id"],
                    map_index=context["task_instance"].map_index,
                    use_event=False,
                    poke_interval=self.poke_interval,
                ),
                timeout=self.execution_timeout,
                method_name=self.execute_complete.__name__,
            )
        return None

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

    # def _use_event(self) -> bool:
    #     if not self.invocation_mode:
    #         self._discover_invocation_mode()
    #     return self.invocation_mode == InvocationMode.DBT_RUNNER and EventMsg is not None

    # minor change, removing event
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
            return bool(self._fallback_to_local_run(try_number, context))

        # We have assumption here that both the build producer and the sensor task will have same invocation mode
        producer_task_state = self._get_producer_task_status(context)
        status = get_xcom_val(ti, self.producer_task_id, f"{self.model_unique_id.replace('.', '__')}_status")

        if status is None:

            if producer_task_state == "failed":
                if self.poke_retry_number > 0:
                    raise AirflowException(
                        f"The dbt build command failed in producer task. Please check the log of task {self.producer_task_id} for details."
                    )
                else:
                    return True  # TODO: remove me
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
class DbtBuildWatcherKubernetesOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherKubernetesOperator(DbtSeedMixin, DbtConsumerKubernetesWatcherSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerKubernetesWatcherSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherKubernetesOperator(DbtSnapshotMixin, DbtConsumerKubernetesWatcherSensor):  # type: ignore[misc]
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerKubernetesWatcherSensor.template_fields


class DbtSourceWatcherKubernetesOperator(DbtSourceKubernetesOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.LOCAL.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceKubernetesOperator.template_fields)  # type: ignore[arg-type]


class DbtRunWatcherKubernetesOperator(DbtConsumerKubernetesWatcherSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerKubernetesWatcherSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtTestWatcherKubernetesOperator(EmptyOperator):
    """
    As a starting point, this operator does nothing.
    We'll be implementing this operator as part of: https://github.com/astronomer/astronomer-cosmos/issues/1974
    """

    def __init__(self, *args: Any, **kwargs: Any):
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
