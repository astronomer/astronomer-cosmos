from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, List, Tuple

import deprecation

if TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult

from cosmos import __version__ as cosmos_version  # type: ignore[attr-defined]
from cosmos.hooks.subprocess import FullOutputSubprocessResult

DBT_NO_TESTS_MSG = "Nothing to do"
DBT_WARN_MSG = "WARN"
DBT_FRESHNESS_WARN_MSG = "WARN freshness of"


def parse_number_of_warnings_subprocess(result: FullOutputSubprocessResult) -> int:
    """
    Parses the dbt test output message and returns the number of warnings.

    :param result: String containing the output to be parsed.
    :return: An integer value associated with the keyword, or 0 if parsing fails.

    Usage:
    -----
    output_str = "Done. PASS=15 WARN=1 ERROR=0 SKIP=0 TOTAL=16"
    num_warns = parse_output(output_str)
    print(num_warns)
    # Output: 1
    """
    output = result.output
    num = 0
    if DBT_NO_TESTS_MSG not in result.output and DBT_WARN_MSG in result.output:
        try:
            num = int(output.split(f"{DBT_WARN_MSG}=")[1].split()[0])
        except ValueError:
            logging.error(
                f"Could not parse number of {DBT_WARN_MSG}s. Check your dbt/airflow version or if --quiet is not being used"
            )
    return num


# Python 3.13 exposes a deprecated operator, we can replace it in the future
@deprecation.deprecated(
    deprecated_in="1.9",
    removed_in="2.0",
    current_version=cosmos_version,
    details="Use the `cosmos.dbt.runner.parse_number_of_warnings` instead.",
)  # type: ignore[misc]
def parse_number_of_warnings_dbt_runner(result: dbtRunnerResult) -> int:  # type: ignore[misc]
    """Parses a dbt runner result and returns the number of warnings found. This only works for dbtRunnerResult
    from invoking dbt build, compile, run, seed, snapshot, test, or run-operation.
    """
    num = 0
    for run_result in result.result.results:  # type: ignore
        if run_result.status == "warn":
            num += 1
    return num


def extract_freshness_warn_msg(result: FullOutputSubprocessResult) -> Tuple[List[str], List[str]]:
    log_list = result.full_output

    node_names = []
    node_results = []

    for line in log_list:

        if DBT_FRESHNESS_WARN_MSG in line:
            node_name = line.split(DBT_FRESHNESS_WARN_MSG)[1].split(" ")[1]
            node_names.append(node_name)
            node_results.append(line)

    return node_names, node_results


def extract_log_issues(log_list: List[str]) -> Tuple[List[str], List[str]]:
    """
    Extracts warning messages from the log list and returns them as a formatted string.

    This function searches for warning messages in dbt test. It reverses the log list for performance
    improvement. It extracts and formats the relevant information and appends it to a list of warnings.

    :param log_list: List of strings, where each string is a log line from dbt test.
    :return: two lists of strings, the first one containing the test names and the second one
        containing the test results.
    """

    def clean_line(line: str) -> str:
        return line.replace("\x1b[33m", "").replace("\x1b[0m", "").strip()

    test_names = []
    test_results = []
    pattern1 = re.compile(r"\d{2}:\d{2}:\d{2}\s+Warning in test ([\w_]+).*")
    pattern2 = re.compile(r"\d{2}:\d{2}:\d{2}\s+(.*)")

    for line_index, line in enumerate(reversed(log_list)):
        cleaned_line = clean_line(line)

        if "Finished running" in cleaned_line:
            # No need to keep checking the log lines once all warnings are found
            break

        if "Warning in test" in cleaned_line:
            test_name = pattern1.sub(r"\1", cleaned_line)
            # test_result is on the next line by default
            test_result = pattern2.sub(r"\1", clean_line(log_list[-(line_index + 1) + 1]))

            test_names.append(test_name)
            test_results.append(test_result)

    return test_names, test_results


# Python 3.13 exposes a deprecated operator, we can replace it in the future
@deprecation.deprecated(
    deprecated_in="1.9",
    removed_in="2.0",
    current_version=cosmos_version,
    details="Use the `cosmos.dbt.runner.extract_message_by_status` instead.",
)  # type: ignore[misc]
def extract_dbt_runner_issues(
    result: dbtRunnerResult, status_levels: list[str] = ["warn"]
) -> Tuple[List[str], List[str]]:  # type: ignore[misc]
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
