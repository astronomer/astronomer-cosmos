"""``ExecutionMode.WATCHER_AWS_ECS``: the watcher producer runs ``dbt build`` in an ECS task."""

from __future__ import annotations

import json
import queue
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from packaging.version import Version

from cosmos.constants import _ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION
from cosmos.dbt.graph import DbtNode
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators._watcher.producer import (
    CONTEXT_KEY,
    execute_watcher_producer,
    finalize_watcher_producer,
    init_watcher_producer,
)
from cosmos.operators.aws_ecs import (
    DbtBuildAwsEcsOperator,
    DbtRunAwsEcsOperator,
    DbtSourceAwsEcsOperator,
)
from cosmos.operators.base import DbtRunMixin, DbtSeedMixin, DbtSnapshotMixin

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.providers.amazon import __version__ as amazon_provider_version
    from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
    from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
except ImportError as err:  # pragma: no cover
    raise ImportError(
        "Could not import the Amazon provider log fetcher. Ensure you've installed the Amazon Web Services "
        "provider separately or with `pip install astronomer-cosmos[...,aws-ecs]`."
    ) from err

if Version(amazon_provider_version) < _ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION:  # pragma: no cover
    raise ImportError(
        f"ExecutionMode.WATCHER_AWS_ECS requires apache-airflow-providers-amazon >= "
        f"{_ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION}; found {amazon_provider_version}."
    )

logger = get_logger(__name__)

# dbt's last JSON event of a run; once seen, no more log lines will arrive for this task.
_COMMAND_COMPLETED_EVENT = "CommandCompleted"
_DRAIN_POLL_SECONDS = 1.0
_STOPPED = "STOPPED"


def _is_command_completed(message: str) -> bool:
    if _COMMAND_COMPLETED_EVENT not in message:
        return False
    try:
        event = json.loads(message)
    except json.JSONDecodeError:
        return False
    info = event.get("info") if isinstance(event, dict) else None
    return isinstance(info, dict) and info.get("name") == _COMMAND_COMPLETED_EVENT


class WatcherEcsLogFetcher(AwsTaskLogFetcher):  # type: ignore[misc]
    """Queues the dbt container's CloudWatch events for the producer to parse on its own thread.

    After ``stop()``, keeps polling until dbt's ``CommandCompleted`` event has been read or
    ``drain_timeout`` has elapsed.
    """

    def __init__(self, *, drain_timeout: timedelta, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events: queue.SimpleQueue[str] = queue.SimpleQueue()
        self.drain_timeout = drain_timeout
        self.command_completed = False
        self._continuation_token = AwsLogsHook.ContinuationToken()

    def run(self) -> None:
        while not self.is_stopped():
            time.sleep(self.fetch_interval.total_seconds())
            self._poll()
        # The provider's loop returns on stop(), dropping the events written since its last poll:
        # for the watcher, the last NodeFinished events.
        deadline = time.monotonic() + self.drain_timeout.total_seconds()
        while not self.command_completed and time.monotonic() < deadline:
            time.sleep(_DRAIN_POLL_SECONDS)
            self._poll()
        if not self.command_completed:
            self.logger.warning(
                "dbt's %s event was not read within %s after the ECS task stopped; consumers of nodes whose "
                "status never arrived will fall back to running them on their own.",
                _COMMAND_COMPLETED_EVENT,
                self.drain_timeout,
            )

    def _poll(self) -> None:
        # An exception escaping run() would end the thread silently: no more statuses would reach
        # the consumers while the task keeps running.
        try:
            self.collect()
        except Exception:
            self.logger.exception("Failed to read the dbt container's CloudWatch log events; retrying.")

    def collect(self) -> None:
        """Read the events available now, queue their messages and log them as the parent does."""
        for event in self._get_log_events(self._continuation_token):
            message = event["message"]
            self.events.put(message)
            if _is_command_completed(message):
                self.command_completed = True
            self.logger.info(self.event_to_str(event))


class DbtProducerWatcherAwsEcsOperator(DbtBuildAwsEcsOperator):
    """Runs ``dbt build`` in an ECS task and publishes per-node statuses read from CloudWatch.

    Requires ``awslogs_group`` and ``awslogs_stream_prefix``; rejects ``deferrable=True``,
    ``reattach=True`` and ``wait_for_completion=False``. See :ref:`watcher-aws-ecs-execution-mode`.

    :param awslogs_fetch_interval: how often CloudWatch is polled while the task runs; it bounds the
        delay before a consumer can see its node's status. The provider default of 30 seconds is
        meant for displaying logs, so this operator defaults to 5 seconds.
    :param drain_timeout: how long to keep polling after the task stopped, waiting for dbt's
        ``CommandCompleted`` event.
    """

    template_fields: tuple[str, ...] = tuple(DbtBuildAwsEcsOperator.template_fields)

    def __init__(
        self,
        *args: Any,
        awslogs_fetch_interval: timedelta = timedelta(seconds=5),
        drain_timeout: timedelta = timedelta(seconds=60),
        **kwargs: Any,
    ) -> None:
        if kwargs.get("deferrable"):
            raise CosmosValueError(
                "ExecutionMode.WATCHER_AWS_ECS does not support deferrable=True on the producer: node statuses "
                "are read from the CloudWatch log stream while the task runs, which only happens on the "
                "synchronous path."
            )
        # The provider's default is [operators] default_deferrable.
        kwargs["deferrable"] = False
        if kwargs.get("wait_for_completion") is False:
            raise CosmosValueError("ExecutionMode.WATCHER_AWS_ECS requires the producer to wait for completion.")
        if kwargs.get("reattach"):
            raise CosmosValueError(
                "ExecutionMode.WATCHER_AWS_ECS does not support reattach=True: the producer never re-runs or "
                "re-attaches to a dbt build; consumers fall back on their own on retry."
            )
        if not (kwargs.get("awslogs_group") and kwargs.get("awslogs_stream_prefix")):
            raise CosmosValueError(
                "ExecutionMode.WATCHER_AWS_ECS requires awslogs_group and awslogs_stream_prefix in operator_args: "
                "node statuses are read from the dbt container's CloudWatch log stream."
            )
        task_id = init_watcher_producer(self, kwargs)
        self.drain_timeout = drain_timeout
        super().__init__(task_id=task_id, awslogs_fetch_interval=awslogs_fetch_interval, *args, **kwargs)
        finalize_watcher_producer(self)

    def _get_task_log_fetcher(self) -> WatcherEcsLogFetcher:
        if not self.awslogs_group:
            raise ValueError("must specify awslogs_group to fetch task logs")
        # Amazon provider 9.34.0 stopped defaulting awslogs_region to the operator's region and
        # resolves it here instead.
        resolve_awslogs_region = getattr(self, "resolve_awslogs_region", None)
        return WatcherEcsLogFetcher(
            drain_timeout=self.drain_timeout,
            aws_conn_id=self.aws_conn_id,
            region_name=resolve_awslogs_region() if resolve_awslogs_region else self.awslogs_region,
            log_group=self.awslogs_group,
            log_stream_name=self._get_logs_stream_name(),
            fetch_interval=self.awslogs_fetch_interval,
            logger=self.log,
        )

    def _process_log_events(self) -> None:
        """Parse the queued dbt log lines and push node statuses; runs on the operator's thread."""
        fetcher = self.task_log_fetcher
        if not isinstance(fetcher, WatcherEcsLogFetcher):
            return
        context = self._context_holder.get(CONTEXT_KEY)
        extra_kwargs = {"context": context} if context else {}
        while True:
            try:
                line = fetcher.events.get_nowait()
            except queue.Empty:
                return
            store_dbt_resource_status_from_log(
                line,
                extra_kwargs,
                tests_per_model=self._tests_per_model,
                test_results_per_model=self._test_results_per_model,
                model_outlet_uris=self._model_outlet_uris,
                should_generate_model_uris=self._should_generate_model_uris,
                upstream_failure_skipped_ids=self._upstream_failure_skipped_ids,
            )

    def _wait_for_task_ended(self) -> None:
        """Poll the task status, parsing the queued log lines between polls.

        Replaces the provider's blocking ``tasks_stopped`` waiter so the lines collected by the
        fetcher thread are parsed here, on the operator's thread, while the build is running. Raises
        ``WaiterError`` as that waiter does, so the provider still stops the task on failure
        (``stop_task_on_failure``, amazon provider 9.22.0 and later).
        """
        from botocore.exceptions import WaiterError

        if not self.client or not self.arn:
            return
        response: dict[str, Any] = {}
        for _ in range(self.waiter_max_attempts):
            self._process_log_events()
            response = self.client.describe_tasks(cluster=self.cluster, tasks=[self.arn])
            tasks = response.get("tasks") or []
            if not tasks:
                raise WaiterError(
                    "TasksStopped", f"ECS task {self.arn} was not found in cluster {self.cluster}", response
                )
            if tasks[0].get("lastStatus") == _STOPPED:
                return
            time.sleep(self.waiter_delay)
        raise WaiterError(
            "TasksStopped",
            f"ECS task {self.arn} did not stop within {self.waiter_max_attempts} polls of {self.waiter_delay}s",
            response,
        )

    def _after_execution(self) -> None:
        # Runs after ``EcsRunTaskOperator.execute`` has stopped and joined the fetcher: the drained
        # tail of the log stream is still in the queue.
        self._process_log_events()
        super()._after_execution()

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # Bind before passing, because bare super() doesn't work inside lambdas or when called outside this method.
        parent_execute = super().execute
        try:
            return execute_watcher_producer(self, context, parent_execute, **kwargs)
        except Exception:
            # When _wait_for_task_ended raises, the provider skips _after_execution: publish the
            # statuses already read, so the consumers of nodes that finished do not fail with the producer.
            self._process_log_events()
            raise


class DbtConsumerWatcherAwsEcsSensor(BaseConsumerSensor, DbtRunAwsEcsOperator):
    """Consumer sensor for ``ExecutionMode.WATCHER_AWS_ECS``.

    Polls the producer's per-node status XCom and, on successful model completion, emits one
    Airflow Asset per outlet URI the producer computed from the manifest. On retry, or when the
    producer finished without reporting this node, runs ``dbt run --select <model>`` in an ECS task
    through ``DbtRunAwsEcsOperator``.
    """

    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + tuple(DbtRunAwsEcsOperator.template_fields)

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run the fallback ECS task synchronously, whatever ``deferrable`` is.

        ``deferrable`` is the sensor's: it defers while waiting for the producer. A deferred ECS task
        would resume in the sensor's ``execute_complete``, which reads the trigger's ``success`` (the
        task stopped) as the node's status, without checking the container's exit code.
        """
        deferrable, self.deferrable = self.deferrable, False
        try:
            return super().build_and_run_cmd(
                context, cmd_flags=cmd_flags, run_as_async=run_as_async, async_context=async_context, **kwargs
            )
        finally:
            self.deferrable = deferrable


# This Operator does not seem to make sense for this particular execution mode, since build is executed by the producer task.
# That said, it is important to raise an exception if users attempt to use TestBehavior.BUILD, until we have a better experience.
class DbtBuildWatcherAwsEcsOperator:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(
            "`ExecutionMode.WATCHER_AWS_ECS` does not expose a DbtBuild operator, since the build command is executed by the producer task."
        )


class DbtSeedWatcherAwsEcsOperator(DbtSeedMixin, DbtConsumerWatcherAwsEcsSensor):
    """
    Watches for the progress of dbt seed execution, run by the producer task (DbtProducerWatcherAwsEcsOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherAwsEcsSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherAwsEcsOperator(DbtSnapshotMixin, DbtConsumerWatcherAwsEcsSensor):
    """
    Watches for the progress of dbt snapshot execution, run by the producer task (DbtProducerWatcherAwsEcsOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherAwsEcsSensor.template_fields


class DbtSourceWatcherAwsEcsOperator(DbtSourceAwsEcsOperator):
    """
    Executes a dbt source freshness command, synchronously, as ExecutionMode.AWS_ECS.
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceAwsEcsOperator.template_fields)


class DbtRunWatcherAwsEcsOperator(DbtConsumerWatcherAwsEcsSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherAwsEcsOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherAwsEcsSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWatcherAwsEcsOperator(DbtConsumerWatcherAwsEcsSensor):
    """Sensor that watches the aggregated test status for a dbt model in WATCHER_AWS_ECS execution mode.

    The producer collects individual test results as they finish and, once every test for a model has
    reported, pushes a single aggregated XCom (``"pass"`` or ``"fail"``) under
    ``get_tests_status_xcom_key(model_uid)``. On manual clear or Airflow-level retry the sensor falls
    back to an ECS task running ``dbt test --select <model>``.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherAwsEcsSensor.template_fields

    # See DbtTestWatcherOperator for the reasoning behind hardcoding base_cmd
    # rather than inheriting from DbtTestMixin.
    base_cmd = ["test"]

    @property
    def is_test_sensor(self) -> bool:
        return True

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """Run ``dbt test --select <model>`` in an ECS task for this model.

        Producer flags are intentionally not forwarded because some of them (e.g. ``--full-refresh``)
        are not valid for ``dbt test``.
        """
        logger.info(
            "Running tests for model '%s' from project '%s' (try %s)",
            self.model_unique_id,
            self.project_dir,
            try_number,
        )
        model_selector = DbtNode.get_resource_name_from_unique_id(self.model_unique_id)
        self.build_and_run_cmd(context, cmd_flags=["--select", model_selector])
        logger.info("dbt test completed successfully for model '%s'", self.model_unique_id)
        return True
