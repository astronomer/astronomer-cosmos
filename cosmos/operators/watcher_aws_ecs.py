from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

try:
    from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
    from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
except ImportError:  # pragma: no cover
    raise ImportError(
        "Could not import EcsRunTaskOperator or AwsTaskLogFetcher. Ensure you've installed the Amazon "
        "Web Services provider separately or with `pip install astronomer-cosmos[...,aws-ecs]`."
    )

from cosmos.log import get_logger
from cosmos.operators._watcher.base import BaseConsumerSensor, store_dbt_resource_status_from_log
from cosmos.operators.aws_ecs import (
    DbtBuildAwsEcsOperator,
    DbtRunAwsEcsOperator,
    DbtSourceAwsEcsOperator,
)
from cosmos.operators.base import (
    DbtRunMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
)

logger = get_logger(__name__)


class DbtAwsTaskLogFetcher(AwsTaskLogFetcher):  # type: ignore[misc]
    """
    Extends ``AwsTaskLogFetcher`` to intercept each CloudWatch log line as it
    arrives and forward it to a caller-supplied ``on_log_line`` callback.

    The base class polls CloudWatch on ``fetch_interval``, logs each event via
    ``self.log.info(self.event_to_str(log_event))``, and then sleeps.  We
    preserve that behaviour exactly and additionally call ``on_log_line`` with
    the raw message so that ``store_dbt_resource_status_from_log`` can parse
    dbt JSON log lines and push per-node status to XCom *while the ECS task is
    still running*.
    """

    def __init__(
        self,
        *args: Any,
        on_log_line: Callable[[str], None],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.log.debug("Initializing DbtAwsTaskLogFetcher with args %s kwargs %s", args, kwargs)
        self.on_log_line = on_log_line

    def run(self) -> None:
        continuation_token = AwsLogsHook.ContinuationToken()
        self.log.info(
            "Starting DbtAwsTaskLogFetcher with log group '%s' and stream '%s'", self.log_group, self.log_stream_name
        )
        while not self.is_stopped():
            time.sleep(self.fetch_interval.total_seconds())
            for log_event in self._get_log_events(continuation_token):
                self.on_log_line(log_event["message"])


class DbtProducerWatcherOperator(DbtBuildAwsEcsOperator):
    """
    Executes a full ``dbt build`` command on AWS ECS and publishes per-node
    status to Airflow XCom as each model / seed / snapshot finishes, so that
    downstream consumer sensor tasks can start as soon as their upstream node
    completes — even while other nodes are still running in the same ECS task.

    Log interception works by overriding ``_get_task_log_fetcher`` to return a
    ``DbtAwsTaskLogFetcher`` that calls ``store_dbt_resource_status_from_log``
    on every CloudWatch log line while the ECS task is running.  This requires
    the standard ``awslogs_group``, ``awslogs_region``, and
    ``awslogs_stream_prefix`` parameters that ``EcsRunTaskOperator`` already
    accepts.

    Retries are intentionally disabled: a second ECS task launch could re-run
    nodes that already succeeded.  Consumer tasks handle their own retry logic
    by falling back to a direct ``dbt run`` when ``try_number > 1``.
    """

    template_fields: tuple[str, ...] = tuple(DbtBuildAwsEcsOperator.template_fields)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", "dbt_producer_watcher_operator")

        # Disable retries to avoid running dbt build twice.
        default_args = dict(kwargs.get("default_args", {}) or {})
        default_args["retries"] = 0
        kwargs["deferrable"] = False  # log fetcher path requires non-deferrable
        kwargs["default_args"] = default_args
        kwargs["retries"] = 0

        super().__init__(task_id=task_id, *args, **kwargs)

        # JSON log format is required so store_dbt_resource_status_from_log
        # can parse structured node status lines.
        self.dbt_cmd_flags += ["--log-format", "json"]

        # Stash for use inside _get_task_log_fetcher, which is called by
        # EcsRunTaskOperator.execute() before we have a chance to pass context
        # through any other mechanism.
        self._current_context: Context | None = None

    # ------------------------------------------------------------------
    # build_and_run_cmd override — stash context before ECS execute
    # ------------------------------------------------------------------

    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        # DbtAwsEcsBaseOperator.build_and_run_cmd calls EcsRunTaskOperator.execute(self, context)
        # as a direct unbound class call, bypassing our execute() override entirely.
        # _get_task_log_fetcher is invoked from within that call, so we must stash
        # context here — before super().build_and_run_cmd — rather than in execute().
        self.log.info("Stashing execution context in build_and_run_cmd: %s", context)
        self._current_context = context
        return super().build_and_run_cmd(context, cmd_flags=cmd_flags, **kwargs)

    # ------------------------------------------------------------------
    # Log fetcher override — the core of the watcher pattern for ECS
    # ------------------------------------------------------------------

    def _get_task_log_fetcher(self) -> DbtAwsTaskLogFetcher:
        self.log.debug(
            "_get_task_log_fetcher called — context available: %s, ecs_task_id: %s",
            self._current_context is not None,
            getattr(self, "ecs_task_id", None),
        )
        """
        Return a ``DbtAwsTaskLogFetcher`` instead of the stock
        ``AwsTaskLogFetcher``.

        ``EcsRunTaskOperator.execute()`` calls this method and assigns the
        result to ``self.task_log_fetcher`` before starting the background
        polling thread, so overriding it is sufficient — no patching required.
        """
        if not self.awslogs_group or not self.awslogs_stream_prefix:
            raise AirflowException(
                "DbtProducerWatcherAwsEcsOperator requires 'awslogs_group' and "
                "'awslogs_stream_prefix' to be set so that dbt JSON logs can be "
                "read from CloudWatch and per-node status pushed to XCom incrementally."
            )

        context = self._current_context

        def _on_log_line(message: str) -> None:
            store_dbt_resource_status_from_log(
                message,
                {
                    "context": context,
                    "project_dir": self.project_dir,
                },
            )

        return DbtAwsTaskLogFetcher(
            aws_conn_id=self.aws_conn_id,
            region_name=self.awslogs_region,
            log_group=self.awslogs_group,
            log_stream_name=self._get_logs_stream_name(),
            fetch_interval=self.awslogs_fetch_interval,
            logger=self.log,
            on_log_line=_on_log_line,
        )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(self, context: Context, **kwargs: Any) -> Any:  # type: ignore[override]
        self.log.info("DbtProducerWatcherAwsEcsOperator execute called with context: %s", context)
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("DbtProducerWatcherAwsEcsOperator expects a task instance in the execution context.")

        try_number = getattr(task_instance, "try_number", 1)
        if try_number > 1:
            self.log.info(
                "DbtProducerWatcherAwsEcsOperator does not support Airflow retries. "
                "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
                try_number,
            )
            return None

        # Go through the full mixin chain:
        #   DbtBuildMixin.execute -> build_and_run_cmd (our override, stashes context)
        #   -> DbtAwsEcsBaseOperator.build_and_run_cmd -> build_command (sets self.overrides)
        #   -> EcsRunTaskOperator.execute -> _get_task_log_fetcher (our override)
        return super().execute(context)


# ---------------------------------------------------------------------------
# Consumer sensor base
# ---------------------------------------------------------------------------


class DbtConsumerWatcherAwsEcsSensor(BaseConsumerSensor, DbtRunAwsEcsOperator):
    """
    Base sensor that watches XCom for per-node dbt status written by
    ``DbtProducerWatcherAwsEcsOperator`` and, when a retry is required,
    falls back to running the model directly via a new ECS task.
    """

    template_fields: tuple[str, ...] = tuple(
        BaseConsumerSensor.template_fields + DbtRunAwsEcsOperator.template_fields  # type: ignore[operator]
    )

    def use_event(self) -> bool:
        # ECS watcher uses XCom polling (same approach as the Kubernetes watcher).
        return False


# ---------------------------------------------------------------------------
# This operator does not make sense for WATCHER mode — raise clearly.
# ---------------------------------------------------------------------------


class DbtBuildWatcherAwsEcsOperator:
    """Placeholder that raises NotImplementedError on instantiation."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator for AWS ECS, "
            "since the build command is executed by the producer task "
            "(DbtProducerWatcherAwsEcsOperator)."
        )


# ---------------------------------------------------------------------------
# Concrete consumer operators
# ---------------------------------------------------------------------------


class DbtSeedWatcherAwsEcsOperator(DbtSeedMixin, DbtConsumerWatcherAwsEcsSensor):  # type: ignore[misc]
    """
    Watches for the progress of a dbt seed execution run by the producer task.
    Falls back to running the seed directly on ECS when a retry is detected.
    """

    template_fields: tuple[str, ...] = tuple(  # type: ignore[assignment]
        DbtConsumerWatcherAwsEcsSensor.template_fields + DbtSeedMixin.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotWatcherAwsEcsOperator(DbtSnapshotMixin, DbtConsumerWatcherAwsEcsSensor):  # type: ignore[misc]
    """
    Watches for the progress of a dbt snapshot execution run by the producer task.
    Falls back to running the snapshot directly on ECS when a retry is detected.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherAwsEcsSensor.template_fields  # type: ignore[assignment]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceWatcherAwsEcsOperator(DbtSourceAwsEcsOperator):
    """
    Executes a dbt source freshness command synchronously on ECS.

    Unlike the other watcher consumers this operator is *not* a sensor —
    source freshness does not produce node_status events that can be watched,
    so we run the command directly (same behaviour as ExecutionMode.AWS_ECS).
    """

    template_fields: tuple[str, ...] = tuple(DbtSourceAwsEcsOperator.template_fields)  # type: ignore[arg-type]


class DbtRunWatcherAwsEcsOperator(DbtConsumerWatcherAwsEcsSensor):
    """
    Watches for the progress of a dbt model execution run by the producer task.
    Falls back to running the model directly on ECS when a retry is detected.
    """

    template_fields: tuple[str, ...] = tuple(  # type: ignore[assignment]
        DbtConsumerWatcherAwsEcsSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestWatcherAwsEcsOperator(EmptyOperator):
    """
    No-op test operator for the WATCHER execution mode on AWS ECS.

    Tests are executed as part of the dbt build command in the producer task;
    there is nothing to watch independently.  A proper implementation is
    tracked in https://github.com/astronomer/astronomer-cosmos/issues/1974.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        desired_keys = ("dag", "task_group", "task_id")
        new_kwargs = {key: value for key, value in kwargs.items() if key in desired_keys}
        super().__init__(**new_kwargs)  # type: ignore[no-untyped-call]
