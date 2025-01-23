from __future__ import annotations

import sys
from functools import lru_cache
from typing import TYPE_CHECKING

from cosmos.dbt.project import change_working_directory, environ
from cosmos.exceptions import CosmosDbtRunError
from cosmos.log import get_logger

if "pytest" in sys.modules:
    # We set the cache limit to 0, so nothing gets cached by default when
    # running tests
    cache = lru_cache(maxsize=0)
else:  # pragma: no cover
    try:
        # Available since Python 3.9
        from functools import cache
    except ImportError:
        cache = lru_cache(maxsize=None)


logger = get_logger(__name__)

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
def get_runner() -> dbtRunner:
    """
    Retrieves a dbtRunner instance.
    """
    from dbt.cli.main import dbtRunner

    return dbtRunner()


def run_command(command: list[str], env: dict[str, str], cwd: str) -> dbtRunnerResult:
    """
    Invokes the dbt command programmatically.
    """
    # Exclude the dbt executable path from the command. This step is necessary because we are using the same
    # command that is used by `InvocationMode.SUBPROCESS`, and in that scenario the first command is necessarily the path
    # to the dbt executable.
    cli_args = command[1:]
    with change_working_directory(cwd), environ(env):
        logger.info("Trying to run dbtRunner with:\n %s\n in %s", cli_args, cwd)
        runner = get_runner()
        result = runner.invoke(cli_args)
    return result


def extract_message_by_status(
    result: dbtRunnerResult, status_levels: list[str] = ["warn"]
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
    node_names = []
    node_results = []

    for node_result in result.result.results:  # type: ignore
        if node_result.status in status_levels:
            node_names.append(str(node_result.node.name))
            node_results.append(str(node_result.message))

    return node_names, node_results


def parse_number_of_warnings(result: dbtRunnerResult) -> int:
    """Parses a dbt runner result and returns the number of warnings found. This only works for dbtRunnerResult
    from invoking dbt build, compile, run, seed, snapshot, test, or run-operation.
    """
    num = 0
    for run_result in result.result.results:  # type: ignore
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
