from __future__ import annotations

import json
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
# dbt 2 (dbt-oss) prints "Summary: 4 total | 1 success | 1 warn | 2 error" instead, leaving out the categories that
# are 0.
DBT_V2_SUMMARY_WARN_RE = re.compile(r"Summary:\s*\d+ total\b.*\|\s*(\d+) warn\b")


def _dbt_log_message(line: str) -> str:
    """Return ``info.msg`` of a dbt ``--log-format json`` line, or the line unchanged for text output."""
    try:
        return str(json.loads(line)["info"]["msg"])
    except (ValueError, TypeError, KeyError):
        return line


def parse_number_of_warnings_subprocess(result: FullOutputSubprocessResult) -> int:
    """
    Parse the number of warnings from the output of a dbt command run via subprocess.

    Scans the full output for the run summary line (``Done. ... WARN=N ... TOTAL=N``, or ``Summary: N total | ... |
    N warn`` on dbt 2) instead of only the last line. Newer dbt versions print lines after the summary, so it is not
    guaranteed to be last (issues #1951, #2492, #2014). The summary is matched by shape so a stray ``WARN=<digits>``
    token on any other line is not miscounted.

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
        match = DBT_SUMMARY_WARN_RE.search(line) or DBT_V2_SUMMARY_WARN_RE.search(line)
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

    for line in map(_dbt_log_message, log_list):
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
    It handles the dbt 1.x ``Warning in test`` / ``[WARNING]: in test`` lines and the dbt 2 ``Warned test`` line.

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
    # The timestamp is optional because ``--log-format json`` messages (``info.msg``) do not carry one.
    pattern1 = re.compile(r"(?:\d{2}:\d{2}:\d{2}\s+)?(?:\[WARNING\]:\s*)?(?:Warning )?in test ([\w_]+).*")
    pattern2 = re.compile(r"(?:\d{2}:\d{2}:\d{2}\s+)?(?:\[WARNING\]:\s*)?(.*)")
    # dbt 2 prints "Warned test  <name> (<path>) [1 of 4 in 1.0s]" and no detail line after it.
    pattern_v2 = re.compile(r"(?:\d{2}:\d{2}:\d{2}\s+)?Warned test\s+(\S+)\s*(.*?)\s*(?:\[\d+ of \d+ in [^\]]*\])?$")
    # dbt 1.x prints "Finished running 1 test in ..." before the warnings. Other "Finished running" lines (dbt 1.x and
    # dbt 2 debug output for a single node or test) come earlier in the log and must not stop the scan.
    finished_running_all_tests = re.compile(r"Finished running \d")

    log_list = [_dbt_log_message(line) for line in log_list]

    for line_index, line in enumerate(reversed(log_list)):
        cleaned_line = clean_line(line)

        if finished_running_all_tests.search(cleaned_line):
            # No need to keep checking the log lines once all warnings are found
            break

        match_v2 = pattern_v2.match(cleaned_line)
        if match_v2:
            test_names.append(match_v2.group(1))
            test_results.append(f"Warned test {match_v2.group(1)} {match_v2.group(2)}".strip())
            continue

        match = pattern1.match(cleaned_line)
        if match:
            test_name = match.group(1)
            # test_result is on the next line by default
            test_result = pattern2.sub(r"\1", clean_line(log_list[-(line_index + 1) + 1]))

            test_names.append(test_name)
            test_results.append(test_result)

    return test_names, test_results


def extract_dbt_failure_details(log_list: list[str]) -> list[str]:
    """
    Extract failing dbt nodes and their messages from subprocess output.

    Counterpart of :func:`extract_log_issues`, which handles warnings. Handles the dbt 1.x ``Failure in test`` /
    ``Error in`` lines and the dbt 2 ``Failed test`` line.

    :param log_list: List of strings, where each string is a log line from a dbt command.
    :return: A list of formatted ``"<failure line> <detail>"`` strings, one per failing node.
    """
    # dbt <= 1.11 prints "Failure in test <name> (<path>)"; dbt 1.12 reworded this to
    # "[ERROR]: in test <name> (<path>)" (also used for errored models) and prefixes the detail line the same way.
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    timestamp = re.compile(r"^\d{2}:\d{2}:\d{2}\s+")
    error_prefix = re.compile(r"^\[ERROR\]:\s*")
    marker = re.compile(r"^(?:(?:Failure|Error) in |\[ERROR\]:\s*in )(.*)")

    def clean_line(line: str) -> str:
        return timestamp.sub("", ansi.sub("", line).strip())

    # dbt 2 prints "Failed test  <name> (<path>) [1 of 4 in 1.0s]" for a failing test, and, in text output only, lists
    # the ones that returned rows under "Test Failures" as "Test failed (4 failed row(s)): <name>".
    marker_v2 = re.compile(r"^Failed test\s+(\S+)\s*(\(.*?\))?\s*(?:\[\d+ of \d+ in [^\]]*\])?$")
    failed_rows_v2 = re.compile(r"^Test failed \((.*?)\):\s*(\S+)")

    cleaned = [clean_line(_dbt_log_message(line)) for line in log_list]
    failed_rows = {m.group(2): m.group(1) for m in map(failed_rows_v2.match, cleaned) if m}
    details: list[str] = []
    for index, line in enumerate(cleaned):
        match_v2 = marker_v2.match(line)
        if match_v2:
            name, path = match_v2.group(1), match_v2.group(2)
            details.append(
                " ".join(
                    filter(
                        None,
                        [
                            f"Failure in test {name}",
                            path,
                            failed_rows.get(name) or failed_rows.get(name.rsplit(".", 1)[-1]),
                        ],
                    )
                )
            )
            continue
        match = marker.match(line)
        if match:
            failure = line if not error_prefix.match(line) else f"Error in {match.group(1)}"
            detail = error_prefix.sub("", cleaned[index + 1]) if index + 1 < len(cleaned) else ""
            details.append(f"{failure} {detail}".strip())
    return details


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
