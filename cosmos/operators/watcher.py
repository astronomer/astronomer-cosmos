from __future__ import annotations

import contextlib
import functools
import io
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

from cosmos.config import ProfileConfig
from cosmos.operators._watcher import safe_xcom_push
from cosmos.settings import watcher_dbt_execution_queue

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover
    from airflow.operators.empty import EmptyOperator  # type: ignore[no-redef]

from cosmos.constants import (
    PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
)
from cosmos.log import get_logger
from cosmos.operators._watcher.base import (
    BaseConsumerSensor,
    store_dbt_resource_status_from_log,
)
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

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

try:
    from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_QUEUE
except ImportError:  # pragma: no cover
    from airflow.models.abstractoperator import DEFAULT_QUEUE  # type: ignore[no-redef]

logger = get_logger(__name__)


class DbtProducerWatcherOperator(DbtBuildMixin, DbtLocalBaseOperator):
    """Run dbt build and update XCom with the progress of each model, as part of the *WATCHER* execution mode.

    Executes **one** ``dbt build`` covering the whole selection.

    dbt is invoked with ``--log-format json`` and the invocation mode is auto-discovered at runtime:
    ``InvocationMode.DBT_RUNNER`` is preferred when dbt-core is available in the same environment
    (faster, no subprocess overhead), falling back to ``InvocationMode.SUBPROCESS`` otherwise.
    The user may override this by passing ``invocation_mode`` explicitly — that value takes precedence.

    Both modes feed the same parser (``store_dbt_resource_status_from_log``):
    - SUBPROCESS: each JSON log line from stdout is parsed directly.
    - DBT_RUNNER: each ``EventMsg`` from the dbt callback is serialised to JSON via
      ``google.protobuf.json_format.MessageToJson`` — a transitive dbt-core dependency — and then
      passed through the same parser.

    As each ``NodeFinished`` event arrives the operator pushes the per-model status to XCom under
    key ``<unique_id>_status`` so downstream sensors can react without waiting for the full build
    to complete.
    """

    template_fields = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]
    # Use staticmethod to prevent Python's descriptor protocol from binding the function to `self`
    # when accessed via instance, which would incorrectly pass `self` as the first argument
    _process_log_line_callable: Callable[[str, Any], None] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self.tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self.test_results_per_model: dict[str, list[str]] = {}
        # Do not publish compiled_sql to the producer's rendered_template: it would contain SQL for
        # all models run by the producer, is often truncated in the UI due to size, and is of no use
        # there; individual sensor tasks show the corresponding rendered_template per model.
        kwargs["should_store_compiled_sql"] = False
        kwargs.setdefault("priority_weight", PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT)
        kwargs.setdefault("weight_rule", WATCHER_TASK_WEIGHT_RULE)
        kwargs["queue"] = watcher_dbt_execution_queue or kwargs.get("queue") or DEFAULT_QUEUE
        # invocation_mode is intentionally NOT forced here; the parent's _discover_invocation_mode()
        # picks DBT_RUNNER when available and falls back to SUBPROCESS otherwise.
        # An explicit invocation_mode passed by the caller is preserved as-is.
        super().__init__(task_id=task_id, *args, **kwargs)
        self.log_format = "json"

    def _make_parse_callable(self) -> Callable[[str, Any], None]:
        """Returns store_dbt_resource_status_from_log with the operator's test maps pre-bound."""
        return functools.partial(
            store_dbt_resource_status_from_log,
            tests_per_model=self.tests_per_model,
            test_results_per_model=self.test_results_per_model,
        )

    def run_subprocess(self, command: list[str], env: dict[str, str], cwd: str, **kwargs: Any) -> Any:
        """Wire up per-line JSON log parsing before delegating to the subprocess runner.

        The subprocess hook passes ``{"context": ..., "project_dir": cwd}`` as ``extra_kwargs`` to
        the callable, so no additional closure is needed here.
        """
        self._process_log_line_callable = self._make_parse_callable()
        return super().run_subprocess(command, env, cwd, **kwargs)

    def run_dbt_runner(self, command: list[str], env: dict[str, str], cwd: str, **kwargs: Any) -> Any:
        """Register an EventMsg → JSON → parse callback before delegating to the dbt runner.

        dbt callbacks receive only the ``EventMsg`` protobuf object; context and project_dir are
        captured via closure so the unified ``store_dbt_resource_status_from_log`` parser can be
        reused identically to the SUBPROCESS path.

        ``google.protobuf.json_format`` is a transitive dependency of dbt-core and is always
        available when ``InvocationMode.DBT_RUNNER`` is in use.

        The callback is only registered when ``context`` is present (i.e. during task execution,
        not during auxiliary calls such as ``dbt deps``). Without a context there is no XCom
        backend to push to, so registering a callback would cause it to raise and dbt would emit
        ``GenericExceptionOnRun`` for every node.

        When a callback is registered, dbt's stdout is redirected to a null buffer so that the
        raw ``--log-format json`` lines do not appear in Airflow task logs alongside the
        human-readable messages already emitted by ``_log_dbt_msg`` inside the callback.
        """
        context = kwargs.get("context")
        if context is not None:
            extra_kwargs: dict[str, Any] = {"project_dir": cwd, "context": context}
            parse = self._make_parse_callable()

            def _event_callback(event: Any) -> None:
                from google.protobuf.json_format import MessageToJson

                json_str = MessageToJson(event, preserving_proto_field_name=True)
                parse(json_str, extra_kwargs)

            self._dbt_runner_callbacks = [*(self._dbt_runner_callbacks or []), _event_callback]
            with contextlib.redirect_stdout(io.StringIO()):
                return super().run_dbt_runner(command, env, cwd, **kwargs)
        return super().run_dbt_runner(command, env, cwd, **kwargs)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("DbtProducerWatcherOperator expects a task instance in the execution context")

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            self.log.info(
                "Dbt WATCHER producer task does not support Airflow retries. "
                "Detected attempt #%s; skipping execution to avoid running a second dbt build.",
                try_number,
            )
            return None

        try:
            return_value = super().execute(context=context, **kwargs)
            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            return return_value

        except Exception:
            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            raise


class DbtConsumerWatcherSensor(BaseConsumerSensor, DbtRunLocalOperator):  # type: ignore[misc]
    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtRunLocalOperator.template_fields  # type: ignore[operator]

    def __init__(
        self,
        *,
        project_dir: str,
        profile_config: ProfileConfig | None = None,
        profiles_dir: str | None = None,
        producer_task_id: str = PRODUCER_WATCHER_TASK_ID,
        poke_interval: int = 10,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval,
            profile_config=profile_config,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            producer_task_id=producer_task_id,
            deferrable=deferrable,
            **kwargs,
        )


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
