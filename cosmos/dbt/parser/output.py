from __future__ import annotations

import re
from typing import TYPE_CHECKING

import deprecation

if TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult

from cosmos import __version__ as cosmos_version
from cosmos.hooks.subprocess import FullOutputSubprocessResult

DBT_FRESHNESS_WARN_MSG = "WARN freshness of"
# The dbt run summary line looks like "Done. PASS=15 WARN=1 ERROR=0 SKIP=0 NO-OP=0 TOTAL=16".
# Anchor on its shape (``Done.`` ... ``WARN=<count>`` ... ``TOTAL=<n>``) so that a later non-summary
# line that happens to contain a "WARN=<digits>" token (e.g. a --debug resource/usage report printed
# after the summary) is not mistaken for the warning count.
DBT_SUMMARY_WARN_RE = re.compile(r"Done\..*\bWARN=(\d+)\b.*\bTOTAL=\d+")


def parse_number_of_warnings_subprocess(result: FullOutputSubprocessResult) -> int:
    """
    Parse the number of warnings from the output of a dbt command run via subprocess.

    Scans the full output (every stdout line) for the dbt run summary line
    ``Done. PASS=.. WARN=N .. TOTAL=N`` instead of only the last line. Newer dbt versions print a
    deprecation summary, a ``--debug`` resource report and a "Flushing usage events" line
    *after* the summary, so it is no longer guaranteed to be the final stdout line. Reading
    only the last line silently misses warnings (issues #1951, #2492, #2014). The summary is
    matched by shape (``Done.`` ... ``TOTAL=N``) so a stray ``WARN=<digits>`` token on any other
    line is not miscounted.

    :param result: The result of the dbt command run via subprocess.
    :return: The number of warnings reported by dbt, or 0 if no summary line is found.

    Usage:
    -----
    full_output = ["...", "Done. PASS=15 WARN=1 ERROR=0 SKIP=0 TOTAL=16", "..."]
    result = FullOutputSubprocessResult(exit_code=0, output=full_output[-1], full_output=full_output)
    num_warns = parse_number_of_warnings_subprocess(result)
    print(num_warns)
    # Output: 1
    """
    for line in reversed(result.full_output):
        match = DBT_SUMMARY_WARN_RE.search(line)
        if match:
            return int(match.group(1))
    return 0


# Python 3.13 exposes a deprecated operator, we can replace it in the future
@deprecation.deprecated(
    deprecated_in="1.9",
    removed_in="2.0",
    current_version=cosmos_version,
    details="Use the `cosmos.dbt.runner.parse_number_of_warnings` instead.",
)  # type: ignore[untyped-decorator]
def parse_number_of_warnings_dbt_runner(result: dbtRunnerResult) -> int:
    """Parses a dbt runner result and returns the number of warnings found. This only works for dbtRunnerResult
    from invoking dbt build, compile, run, seed, snapshot, test, or run-operation.
    """
    num = 0
    for run_result in result.result.results:
        if run_result.status == "warn":
            num += 1
    return num


def extract_freshness_warn_msg(result: FullOutputSubprocessResult) -> tuple[list[str], list[str]]:
    log_list = result.full_output

    node_names = []
    node_results = []

    for line in log_list:

        if DBT_FRESHNESS_WARN_MSG in line:
            node_name = line.split(DBT_FRESHNESS_WARN_MSG)[1].split(" ")[1]
            node_names.append(node_name)
            node_results.append(line)

    return node_names, node_results


def extract_log_issues(log_list: list[str]) -> tuple[list[str], list[str]]:
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
    # dbt <= 1.11 prints "HH:MM:SS  Warning in test <name> (<path>)"; dbt 1.12 reworded this to
    # "HH:MM:SS  [WARNING]: in test <name> (<path>)" (and the result line gains the same
    # "[WARNING]: " prefix). Match both so the parser keeps working across dbt versions.
    pattern1 = re.compile(r"\d{2}:\d{2}:\d{2}\s+(?:\[WARNING\]:\s*)?(?:Warning )?in test ([\w_]+).*")
    pattern2 = re.compile(r"\d{2}:\d{2}:\d{2}\s+(?:\[WARNING\]:\s*)?(.*)")

    for line_index, line in enumerate(reversed(log_list)):
        cleaned_line = clean_line(line)

        if "Finished running" in cleaned_line:
            # No need to keep checking the log lines once all warnings are found
            break

        match = pattern1.match(cleaned_line)
        if match:
            test_name = match.group(1)
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
)  # type: ignore[untyped-decorator]
def extract_dbt_runner_issues(
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
            node_names.append(str(node_result.node.name))
            node_results.append(str(node_result.message))

    return node_names, node_results
