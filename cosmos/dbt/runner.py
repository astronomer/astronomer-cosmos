from __future__ import annotations

import gc
import json
import sys
from collections.abc import Callable
from functools import cache as functools_cache
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from cosmos.dbt.project import change_working_directory, environ, exclude_dags_folder_from_sys_path
from cosmos.exceptions import CosmosDbtRunError
from cosmos.log import get_logger

if "pytest" in sys.modules:
    # We set the cache limit to 0, so nothing gets cached by default when
    # running tests
    cache = lru_cache(maxsize=0)
else:  # pragma: no cover
    cache = functools_cache


logger = get_logger(__name__)

# dbt events carrying the macro error text of a failed ``run-operation``.
MACRO_ERROR_EVENTS = ("RunningOperationCaughtError", "RunningOperationUncaughtError")

if TYPE_CHECKING:  # pragma: no cover
    from dbt.cli.main import dbtRunner, dbtRunnerResult


@cache
def is_available() -> bool:
    """
    Checks if the dbt runner is available (if dbt-core is installed in the same Python virtualenv as Airflow)."
    """
    try:
        from dbt.cli.main import dbtRunner  # noqa
    except ImportError:
        return False
    return True


@cache
def _get_cached_dbt_runner() -> dbtRunner:
    """
    Retrieves a dbtRunner instance.
    """
    from dbt.cli.main import dbtRunner

    return dbtRunner()


def get_runner(callbacks: list[Callable] | None = None) -> dbtRunner:  # type: ignore[type-arg]
    """
    Retrieves a dbtRunner instance.
    """
    if callbacks and isinstance(callbacks, list):
        from dbt.cli.main import dbtRunner

        return dbtRunner(callbacks=callbacks)

    return _get_cached_dbt_runner()


def _cleanup_dbt_adapters() -> None:
    """
    Reset dbt adapters to release semaphores.

    dbt adapters maintain internal state that holds onto
    semaphores. Resetting the adapters after each dbt command combined with
    garbage collection prevents "leaked semaphore objects" warnings.

    See: https://github.com/astronomer/astronomer-cosmos/issues/2334
    """
    try:
        from dbt.adapters.factory import reset_adapters

        reset_adapters()
    except ImportError:
        pass
    except (RuntimeError, KeyError, AttributeError):
        logger.debug("Error resetting dbt adapters", exc_info=True)

    gc.collect()


def dbt_event_to_json(event: Any) -> str:
    """Serialise a dbt ``EventMsg`` to JSON so DBT_RUNNER consumers read the SUBPROCESS field names.

    Not byte-identical to a ``--log-format json`` line, so read the payload with ``.get()``.

    ``google.protobuf.json_format`` is a transitive dependency of dbt-core and is always available
    when ``InvocationMode.DBT_RUNNER`` is in use.
    """
    from google.protobuf.json_format import MessageToJson

    return str(MessageToJson(event, preserving_proto_field_name=True))


def _collect_macro_errors(collected: list[str]) -> Callable[[Any], None]:
    def collect(event: Any) -> None:
        # Never raise: dbt wraps a raising callback as GenericExceptionOnRun, which would replace
        # the dbt error this callback exists to surface with the callback's own failure.
        try:
            info = json.loads(dbt_event_to_json(event)).get("info", {})
            if info.get("name") not in MACRO_ERROR_EVENTS:
                return
            msg = info.get("msg")
            if msg:
                collected.append(str(msg))
        except Exception:
            logger.debug("Unable to read a dbt event while collecting macro errors", exc_info=True)

    return collect


def _fill_missing_messages(result: dbtRunnerResult, macro_errors: list[str]) -> None:
    """dbt < 1.12 hardcodes ``message=None`` on run-operation results (dbt-labs/dbt-core#12730)."""
    if not macro_errors:
        return

    node_results = getattr(result.result, "results", None) or []
    if not node_results:
        # Debug log rather than an exception: a Cosmos-side complaint about the result shape would
        # hide the run-operation failure the user came for. It still surfaces the contract change.
        logger.debug(
            "Collected %d dbt macro error event(s) but no run-operation result to attach them to "
            "(result.result.results is missing or empty)",
            len(macro_errors),
        )
        return

    message = "\n".join(macro_errors)
    for node_result in node_results:
        if getattr(node_result, "message", None) is None:
            node_result.message = message


def run_command(
    command: list[str], env: dict[str, str], cwd: str, callbacks: list[Callable] | None = None, **kwargs: Any  # type: ignore[type-arg]
) -> dbtRunnerResult:
    """
    Invokes the dbt command programmatically.
    """
    # Exclude the dbt executable path from the command. This step is necessary because we are using the same
    # command that is used by `InvocationMode.SUBPROCESS`, and in that scenario the first command is necessarily the path
    # to the dbt executable.
    cli_args = command[1:]
    macro_errors: list[str] = []
    # ``build_cmd`` puts flags on both sides of the subcommand — dbt global flags before it,
    # ``add_global_flags()``/``dbt_cmd_flags`` after it — so a positional check is not safe here.
    if "run-operation" in cli_args:
        callbacks = [*(callbacks or []), _collect_macro_errors(macro_errors)]
    # ``exclude_dags_folder_from_sys_path`` must enter *before* ``change_working_directory`` so it
    # resolves ``DAGS_FOLDER`` against the Airflow process cwd. A relative ``DAGS_FOLDER`` resolved
    # after the chdir would point at the dbt project dir and fail to strip the real DAGs folder.
    with exclude_dags_folder_from_sys_path(), change_working_directory(cwd), environ(env):
        logger.info("Trying to run dbtRunner with:\n %s\n in %s", cli_args, cwd)
        runner = get_runner(callbacks=callbacks)
        try:
            result = runner.invoke(cli_args)
        finally:
            # Reset dbt adapters to release semaphores (run on all exit paths)
            # See: https://github.com/astronomer/astronomer-cosmos/issues/2334
            _cleanup_dbt_adapters()

    _fill_missing_messages(result, macro_errors)

    return result


def _node_label(node_result: Any) -> str:
    """``run-operation`` yields ``RunResultOutput``, which has no ``node`` (#2982)."""
    node = getattr(node_result, "node", None)
    return str(getattr(node, "name", None) or getattr(node_result, "unique_id", None) or "unknown")


def extract_message_by_status(
    result: dbtRunnerResult, status_levels: list[str] | None = None
) -> tuple[list[str], list[str]]:
    """
    Extracts messages from the dbt runner result and returns them as a formatted string.

    This function iterates over dbtRunnerResult messages in dbt run. It extracts results that match the
    status levels provided and appends them to a list of issues.

    :param result: dbtRunnerResult object containing the output to be parsed.
    :param status_levels: List of strings, where each string is a result status level. Default is ["warn"].
    :return: two lists of strings, the first one containing the node names and the second one
        containing the node result message.
    """
    status_levels = ["warn"] if status_levels is None else status_levels

    node_names = []
    node_results = []

    for node_result in result.result.results:
        if node_result.status in status_levels:
            node_names.append(_node_label(node_result))
            node_results.append(str(node_result.message))

    return node_names, node_results


def parse_number_of_warnings(result: dbtRunnerResult) -> int:
    """Parses a dbt runner result and returns the number of warnings found. This only works for dbtRunnerResult
    from invoking dbt build, compile, run, seed, snapshot, test, or run-operation.
    """
    num = 0
    for run_result in result.result.results:
        if run_result.status == "warn":
            num += 1
    return num


def handle_exception_if_needed(result: dbtRunnerResult) -> None:
    """
    Given a dbtRunnerResult, identify if it failed and handle the exception, if necessary.
    """
    # dbtRunnerResult has an attribute `success` that is False if the command failed.
    if not result.success:
        if result.exception:
            raise CosmosDbtRunError(f"dbt invocation did not complete with unhandled error: {result.exception}")
        else:
            node_names, node_results = extract_message_by_status(result, ["error", "fail", "runtime error"])
            error_message = "\n".join([f"{name}: {result}" for name, result in zip(node_names, node_results)])
            raise CosmosDbtRunError(f"dbt invocation completed with errors: {error_message}")
