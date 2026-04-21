from __future__ import annotations

import contextlib
import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowSkipException

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos.config import ProfileConfig
from cosmos.constants import (
    PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    PRODUCER_WATCHER_TASK_ID,
    WATCHER_TASK_WEIGHT_RULE,
    DbtResourceType,
)
from cosmos.dataset import get_dataset_namespace
from cosmos.dbt.graph import DbtNode
from cosmos.log import get_logger
from cosmos.operators._watcher import safe_xcom_push
from cosmos.operators._watcher.base import (
    BaseConsumerSensor,
    store_dbt_resource_status_from_log,
)
from cosmos.operators._watcher.state import DBT_SUCCESS_STATUSES
from cosmos.operators._watcher.xcom import (
    _backup_xcom_to_variable,
    _delete_xcom_backup_variable,
    _init_xcom_backup,
    _restore_xcom_from_variable,
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
from cosmos.settings import watcher_dbt_execution_queue

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


def _default_freshness_callback(
    context: Context,
    dag: Any,
    task_group: TaskGroup | None,
    nodes: dict[str, DbtNode] | None,
    sources_json: dict[str, Any] | None,
) -> list[tuple[str, str]]:
    """Return a list of ``(unique_id, state)`` tuples for nodes that must be skipped due to stale sources.

    Stale sources are those with ``status`` of ``"error"`` or ``"warn"`` in ``sources_json["results"]``.

    A node is skipped only when **all** of its upstream dependencies are either stale sources or
    already-skipped nodes.  If a node has at least one clean upstream path it may still execute
    successfully — for example when a model depends on both a stale source and a clean model — so
    it is excluded from the skip set and allowed to run.

    Traversal is a DFS over the reverse-dependency graph built from ``nodes``.
    """
    if not nodes or not sources_json:
        return []

    stale_source_ids = {r["unique_id"] for r in sources_json.get("results", []) if r.get("status") in ("error", "warn")}
    if not stale_source_ids:
        return []

    # Build reverse map: dep_id -> set of node_ids that directly depend on it
    dependents: dict[str, set[str]] = {}
    for node_id, node in nodes.items():
        for dep_id in node.depends_on:
            dependents.setdefault(dep_id, set()).add(node_id)

    # DFS from each stale source.  A dependent is added to the skip set only when every entry in
    # its depends_on is either a known-stale source or already in the skip set.  This preserves
    # nodes that have at least one clean upstream path: they may succeed and should not be
    # preemptively excluded.  When a new node is added to visited its own dependents are queued
    # so they can be re-evaluated with the updated skip set.
    _excludable_resource_types = {DbtResourceType.MODEL, DbtResourceType.SEED, DbtResourceType.SNAPSHOT}
    visited: set[str] = set()
    queue = list(stale_source_ids)
    while queue:
        current = queue.pop()
        for dependent_id in dependents.get(current, set()):
            if dependent_id in visited:
                continue
            dependent_node = nodes.get(dependent_id)
            if dependent_node is None:
                continue
            if all(dep in stale_source_ids or dep in visited for dep in dependent_node.depends_on):
                visited.add(dependent_id)
                queue.append(dependent_id)

    # Only return model/seed/snapshot nodes — tests are skipped automatically when their parent is excluded,
    # and test hash-suffixed unique_ids are not valid dbt --exclude selectors.
    excludable = [uid for uid in visited if nodes.get(uid) and nodes[uid].resource_type in _excludable_resource_types]
    logger.info("Nodes to skip due to stale sources: %s", excludable)
    return [(uid, "skipped") for uid in excludable]


class _NullWriter:
    """Write-only sink that discards all data; used to suppress dbt stdout in DBT_RUNNER mode.

    Preferred over ``io.StringIO()`` because StringIO buffers every byte written to it for the
    lifetime of the context manager. On large projects dbt emits megabytes of JSON log lines,
    so StringIO would grow unbounded and increase worker memory usage proportionally to project
    size and verbosity. _NullWriter discards each write immediately with no allocation.
    """

    def write(self, s: str) -> int:
        return len(s)

    def flush(self) -> None:
        pass


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

    When the private kwarg ``_check_source_freshness`` is ``True`` (set automatically by
    ``_add_watcher_producer_task`` when ``SourceRenderingBehavior`` is not ``NONE``), the
    producer first runs ``dbt source freshness``, identifies stale sources, marks all
    transitive dependents as ``"skipped"`` via XCom, and adds them to ``--exclude`` before
    running the main ``dbt build``.  An optional ``freshness_callback`` kwarg
    (exposed via ``RenderConfig.freshness_callback``) may be passed to override the default
    skip-propagation logic.
    """

    template_fields = DbtLocalBaseOperator.template_fields + DbtBuildMixin.template_fields  # type: ignore[operator]
    # Use staticmethod to prevent Python's descriptor protocol from binding the function to `self`
    # when accessed via instance, which would incorrectly pass `self` as the first argument
    _process_log_line_callable: Callable[[str, Any], None] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        task_id = kwargs.pop("task_id", PRODUCER_WATCHER_TASK_ID)
        self.tests_per_model: dict[str, list[str]] = kwargs.pop("tests_per_model", {})
        self.test_results_per_model: dict[str, list[str]] = {}
        self._check_source_freshness: bool = kwargs.pop("_check_source_freshness", False)
        self._freshness_callback: Callable[
            [Context, Any, TaskGroup | None, dict[str, DbtNode] | None, dict[str, Any] | None],
            list[tuple[str, str]],
        ] = kwargs.pop("freshness_callback", _default_freshness_callback)
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

        # Mutable dict populated lazily from the manifest; shared with the log parser.
        self._dataset_namespace: str | None = None
        self._model_outlet_uris: dict[str, list[str]] = {}

    def _handle_datasets(self, context: Context) -> None:
        """No-op override: consumer tasks handle their own dataset emission in WATCHER mode."""

    def _make_parse_callable(self) -> Callable[[str, Any], None]:
        """Returns store_dbt_resource_status_from_log with the operator's test maps pre-bound."""
        return functools.partial(
            store_dbt_resource_status_from_log,
            tests_per_model=self.tests_per_model,
            test_results_per_model=self.test_results_per_model,
            model_outlet_uris=self._model_outlet_uris,
            dataset_namespace=self._dataset_namespace,
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

        The callback is intentionally **not** registered during the source freshness pre-check
        (``context["_check_source_freshness"] is True``).  Registering it there would leave a
        stale entry in ``_dbt_runner_callbacks`` that fires again for every event during the
        subsequent ``dbt build``, producing duplicate log lines.  Freshness results are read from
        ``target/sources.json`` after the run and do not need per-event XCom pushes.
        """
        context = kwargs.get("context")
        if context is not None:
            # During the source freshness pre-check suppress raw JSON stdout, but do not register
            # the XCom-pushing callback so it cannot accumulate and duplicate build logs later.
            if context.get("_check_source_freshness"):
                with contextlib.redirect_stdout(_NullWriter()):
                    return super().run_dbt_runner(command, env, cwd, **kwargs)

            extra_kwargs: dict[str, Any] = {"project_dir": cwd, "context": context}
            parse = self._make_parse_callable()
            # Collect callback errors rather than raising inside the callback: dbt catches
            # exceptions raised by callbacks and wraps them as GenericExceptionOnRun, which
            # would cause the build to emit spurious failures but potentially still succeed.
            # Instead we capture the first error here and re-raise it after the dbt run so it
            # propagates through execute(), triggering the existing task_status XCom mechanism
            # that signals consumer sensors to check the producer task state.
            callback_error: list[BaseException] = []

            def _event_callback(event: Any) -> None:
                try:
                    from google.protobuf.json_format import MessageToJson

                    json_str = MessageToJson(event, preserving_proto_field_name=True)
                    parse(json_str, extra_kwargs)
                except Exception as e:
                    logger.exception("Error in dbt event callback: %s", e)
                    if not callback_error:
                        callback_error.append(e)

            self._dbt_runner_callbacks = [*(self._dbt_runner_callbacks or []), _event_callback]
            with contextlib.redirect_stdout(_NullWriter()):
                result = super().run_dbt_runner(command, env, cwd, **kwargs)
            if callback_error:
                raise callback_error[0]
            return result
        return super().run_dbt_runner(command, env, cwd, **kwargs)

    def _push_node_state_xcom(self, ti: Any, unique_id: str, state: str) -> None:
        """Push a synthetic status XCom for a node using the given ``state``.

        Uses the unified ``*_status`` XCom key that consumer sensors already poll.
        """
        uid_key = unique_id.replace(".", "__")
        safe_xcom_push(task_instance=ti, key=f"{uid_key}_status", value={"status": state, "outlet_uris": []})

    def _run_source_freshness(self, context: Context) -> None:
        """Run ``dbt source freshness`` via ``build_cmd`` and ``run_command``.

        Temporarily overrides operator attributes that carry flags unsupported by
        ``dbt source freshness`` (``--full-refresh``, ``--indirect-selection``,
        and build-specific ``dbt_cmd_flags`` such as ``--resource-type``).
        ``--select``/``--exclude`` are unaffected (they come from ``add_global_flags``).
        """
        original_base_cmd = self.base_cmd
        original_indirect_selection = getattr(self, "indirect_selection", None)
        original_full_refresh = getattr(self, "full_refresh", None)
        original_dbt_cmd_flags = self.dbt_cmd_flags
        try:
            self.base_cmd = ["source", "freshness"]
            self.indirect_selection = None  # ``dbt source freshness`` does not support --indirect-selection
            self.full_refresh = False  # type: ignore[assignment]  # ``dbt source freshness`` does not support --full-refresh
            self.dbt_cmd_flags = []  # clear build-specific flags (e.g. --resource-type)
            full_cmd, env = self.build_cmd(context=context, cmd_flags=self.add_cmd_flags())
            context["_check_source_freshness"] = True  # type: ignore[typeddict-unknown-key]
            self.run_command(cmd=full_cmd, env=env, context=context)
        finally:
            self.base_cmd = original_base_cmd
            self.indirect_selection = original_indirect_selection
            self.full_refresh = original_full_refresh  # type: ignore[assignment]
            self.dbt_cmd_flags = original_dbt_cmd_flags
            context.pop("_check_source_freshness", None)  # type: ignore[typeddict-item]

    def _apply_node_state_tokens(self, context: Context, node_state_pairs: list[tuple[str, str]]) -> None:
        if not node_state_pairs:
            return

        ti = context["ti"]

        for unique_id, state in node_state_pairs:
            logger.info("Pre-setting resource '%s' state to %s from source-freshness callback", unique_id, state)
            self._push_node_state_xcom(ti, unique_id, state)

        # Exclude any node whose pre-set state is non-success from the dbt build.
        # This covers both "skipped" (default) and failure states ("failed", "fail",
        # "error") that a custom freshness_callback may return.  Without exclusion,
        # dbt would run the model anyway and either overwrite the pre-set XCom status
        # or trigger a race condition with the consumer sensor.
        # Use the same parsing as DbtNode.resource_name: unique_id.split(".", 2)[2]
        # This preserves version suffixes (e.g. model.pkg.my_model.v1 -> my_model.v1)
        excluded_ids = [uid for uid, state in node_state_pairs if state not in DBT_SUCCESS_STATUSES]
        if not excluded_ids:
            return
        model_names = sorted({uid.split(".", 2)[2] for uid in excluded_ids if len(uid.split(".", 2)) == 3})
        exclude_str = " ".join(model_names)
        if exclude_str:
            current_exclude = getattr(self, "exclude", None)
            self.exclude = f"{current_exclude} {exclude_str}" if current_exclude else exclude_str

    def _push_source_freshness_results(self, context: Context) -> None:
        """Push per-source freshness status to XCom so source consumer sensors can read it."""
        if not self._sources_json:
            return
        ti = context["ti"]
        for result in self._sources_json.get("results", []):
            unique_id = result.get("unique_id")
            status = result.get("status")
            if unique_id and status:
                uid_key = unique_id.replace(".", "__")
                safe_xcom_push(
                    task_instance=ti,
                    key=f"{uid_key}_status",
                    value={"status": status, "outlet_uris": []},
                )

    def _apply_source_freshness(self, context: Context) -> None:
        """Run source freshness, invoke the callback, and mark affected nodes as skipped."""
        self._run_source_freshness(context)

        # Push per-source freshness results so source consumer sensors can read them
        self._push_source_freshness_results(context)

        dag = context.get("dag")
        task_group = getattr(context.get("task_instance"), "task", None)
        task_group = getattr(task_group, "task_group", None)

        # Use the full graph (nodes) for dependency traversal so intermediate unselected
        # nodes don't break transitive relationships.  The callback intersects the result
        # with rendered resource types so only actionable nodes are returned.
        nodes = None
        if dag is not None:
            dbt_graph = getattr(dag, "dbt_graph", None)
            nodes = getattr(dbt_graph, "nodes", None)
        if nodes is None and task_group is not None:
            tg_dbt_graph = getattr(task_group, "dbt_graph", None)
            nodes = getattr(tg_dbt_graph, "nodes", None)

        freshness_results = self._freshness_callback(context, dag, task_group, nodes, self._sources_json)
        self._apply_node_state_tokens(context, freshness_results)

    def execute(self, context: Context, **kwargs: Any) -> Any:
        # Pre-compute the dataset namespace for per-model outlet URI generation.
        self._dataset_namespace = get_dataset_namespace(self.profile_config)
        self._model_outlet_uris.clear()

        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("DbtProducerWatcherOperator expects a task instance in the execution context")

        try_number = getattr(task_instance, "try_number", 1)

        if try_number > 1:
            _restore_xcom_from_variable(context)
            raise AirflowSkipException(
                "Dbt WATCHER producer task does not support Airflow retries. "
                f"Detected attempt #{try_number}; skipping execution to avoid running a second dbt build."
            )

        _init_xcom_backup(context)

        if self._check_source_freshness:
            self._apply_source_freshness(context)

        try:
            return_value = super().execute(context=context, **kwargs)
            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            _delete_xcom_backup_variable(context)
            return return_value

        except Exception:
            safe_xcom_push(task_instance=context["ti"], key="task_status", value="completed")
            _backup_xcom_to_variable(context)
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

    def _emit_datasets(self, context: Context) -> None:
        """Emit Airflow datasets for this consumer task's model using outlet URIs from the producer."""
        if not getattr(self, "emit_datasets", False):
            return
        outlet_uris = getattr(self, "_outlet_uris", [])
        if not outlet_uris:
            return

        from cosmos import settings
        from cosmos.constants import AIRFLOW_VERSION

        if AIRFLOW_VERSION.major >= 3:
            from airflow.sdk.definitions.asset import Asset
        else:
            from airflow.datasets import Dataset as Asset  # type: ignore[no-redef]

        outlets = [Asset(uri=uri) for uri in outlet_uris]
        logger.info("Emitting %d dataset(s) for model '%s': %s", len(outlets), self.model_unique_id, outlet_uris)
        self.register_dataset([], outlets, context)

        if settings.enable_uri_xcom:
            context["ti"].xcom_push(key="uri", value=outlet_uris)

    def execute(self, context: Context, **kwargs: Any) -> None:  # type: ignore[override]
        super().execute(context, **kwargs)
        # If we reach here without deferring, the model succeeded — emit datasets
        self._emit_datasets(context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        # Extract outlet URIs from trigger event before parent handles status
        self._outlet_uris = event.get("outlet_uris", [])
        super().execute_complete(context, event)
        # If we reach here without raising, the model succeeded — emit datasets
        self._emit_datasets(context)


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


class DbtSourceWatcherOperator(BaseConsumerSensor, DbtSourceLocalOperator):  # type: ignore[misc]
    """Watches for source freshness results from the producer task.

    When the producer has ``_check_source_freshness`` enabled it runs
    ``dbt source freshness`` and pushes per-source status to XCom.
    This sensor reads that status.  On retry (or when the producer did
    not provide a result) it falls back to running ``dbt source freshness``
    locally for its specific source.
    """

    template_fields: tuple[str, ...] = BaseConsumerSensor.template_fields + DbtSourceLocalOperator.template_fields  # type: ignore[operator]

    @property
    def _resource_label(self) -> str:
        """Human-readable label for this sensor's dbt resource type."""
        return "Source"

    def _fallback_to_non_watcher_run(self, try_number: int, context: Context) -> bool:
        """Run ``dbt source freshness`` locally for this specific source on retry."""
        logger.info(
            "Retry attempt #%s – Running source freshness for '%s' from project '%s'",
            try_number - 1,
            self.model_unique_id,
            self.project_dir,
        )
        resource_name = self.model_unique_id.split(".", 2)[2]
        cmd_flags = ["--select", f"source:{resource_name}"]
        self.build_and_run_cmd(context, cmd_flags=cmd_flags)  # type: ignore[attr-defined]
        logger.info("dbt source freshness completed successfully on retry for source '%s'", self.model_unique_id)
        return True


class DbtRunWatcherOperator(DbtConsumerWatcherSensor):
    """
    Watches for the progress of dbt model execution, run by the producer task (DbtProducerWatcherOperator).
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherSensor.template_fields + DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class DbtTestWatcherOperator(DbtConsumerWatcherSensor):  # type: ignore[misc]
    """Sensor that watches the aggregated test status for a dbt model in watcher execution mode.

    The producer task (``DbtProducerWatcherOperator``) collects individual test
    results as they finish and, once every test for a given model has reported,
    pushes a single aggregated XCom (``"pass"`` or ``"fail"``) under the key
    ``<model_unique_id>_tests_status``.

    This sensor polls that key and:
    * returns success when the value is ``"pass"``,
    * raises ``AirflowException`` when the value is ``"fail"``.

    Deferral is fully supported: the ``WatcherTrigger`` receives
    ``is_test_sensor=True`` and polls the correct aggregated key.
    """

    template_fields: tuple[str, ...] = DbtConsumerWatcherSensor.template_fields  # type: ignore[operator]

    @property
    def is_test_sensor(self) -> bool:
        return True
