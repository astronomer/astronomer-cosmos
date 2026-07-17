from unittest.mock import MagicMock

import pytest

from cosmos.dbt.parser.output import (
    extract_dbt_runner_issues,
    extract_freshness_warn_msg,
    extract_log_issues,
    parse_number_of_warnings_dbt_runner,
    parse_number_of_warnings_subprocess,
)
from cosmos.hooks.subprocess import FullOutputSubprocessResult


def _subprocess_result(full_output: list[str]) -> FullOutputSubprocessResult:
    """Build a FullOutputSubprocessResult whose ``output`` is the last line, mirroring how
    FullOutputSubprocessHook.run_command populates it in production."""
    return FullOutputSubprocessResult(exit_code=0, output=full_output[-1], full_output=full_output)


@pytest.mark.parametrize(
    "full_output, expected_warnings",
    [
        (["Done. PASS=15 WARN=1 ERROR=0 SKIP=0 TOTAL=16"], 1),
        (["Done. PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=16"], 0),
        (["Done. PASS=15 WARN=2 ERROR=0 SKIP=0 TOTAL=16"], 2),
        (["Nothing to do. Exiting without running tests."], 0),
    ],
)
def test_parse_number_of_warnings_subprocess(full_output: list[str], expected_warnings: int):
    num_warns = parse_number_of_warnings_subprocess(_subprocess_result(full_output))
    assert num_warns == expected_warnings


def test_parse_number_of_warnings_subprocess_summary_not_last_line():
    """Regression for issues #1951, #2492 and #2014.

    Newer dbt versions print a deprecation summary, a ``--debug`` resource report and a
    "Flushing usage events" line *after* the ``Done. ... WARN=N ...`` summary, so the summary is
    no longer the last stdout line. The previous implementation inspected only the last line and
    silently returned 0, so on_warning_callback was never invoked.
    """
    full_output = [
        "13:52:42  1 of 1 WARN 1 not_null_my_model_my_column .................. [WARN 1 in 2.53s]",
        "13:52:44  Done. PASS=0 WARN=1 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1",
        "13:52:44  [WARNING][DeprecationsSummary]: Deprecated functionality",
        "Summary of encountered deprecations:",
        "- PropertyMovedToConfigDeprecation: 12 occurrences",
        '13:52:44  Resource report: {"command_name": "test", "command_success": true}',
        "13:52:44  Command `dbt test` succeeded at 13:52:44 after 39.75 seconds",
        "13:52:44  Flushing usage events",
    ]
    assert parse_number_of_warnings_subprocess(_subprocess_result(full_output)) == 1


def test_parse_number_of_warnings_subprocess_without_summary_returns_zero():
    """A run without a ``WARN=<count>`` summary line (e.g. dbt ``--quiet``) parses as 0 and does
    not raise, even when a line contains the bare substring ``WARN=``."""
    full_output = ["Running with dbt=1.11.8", "WARN= is not a valid summary token"]
    assert parse_number_of_warnings_subprocess(_subprocess_result(full_output)) == 0


def test_parse_number_of_warnings_subprocess_only_counts_summary_line():
    """Only the ``Done. ... WARN=N ... TOTAL=N`` summary line is counted. Because the full output is
    scanned in reverse, a later non-summary line that happens to contain a ``WARN=<digits>`` token
    (e.g. a ``--debug`` resource/usage report printed after the summary) must not override the real
    summary, nor be counted when there is no summary line at all (PR #2832 review)."""
    # A trailing WARN=<digits> emitted after the real summary is ignored; the summary's count wins.
    after_summary = [
        "13:52:44  Done. PASS=0 WARN=1 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1",
        "13:52:45  Resource report: emitted WARN=99 internal-metric events",
        "13:52:45  Flushing usage events",
    ]
    assert parse_number_of_warnings_subprocess(_subprocess_result(after_summary)) == 1

    # A stray WARN=<digits> with no Done./TOTAL= summary line counts as 0 (callback not triggered).
    no_summary = [
        "Running with dbt=1.11.8",
        "13:52:45  Resource report: emitted WARN=99 internal-metric events",
    ]
    assert parse_number_of_warnings_subprocess(_subprocess_result(no_summary)) == 0


def test_parse_number_of_warnings_dbt_runner_with_warnings():
    runner_result = MagicMock()
    runner_result.result.results = [
        MagicMock(status="pass"),
        MagicMock(status="warn"),
        MagicMock(status="pass"),
        MagicMock(status="warn"),
    ]
    num_warns = parse_number_of_warnings_dbt_runner(runner_result)
    assert num_warns == 2


def test_extract_log_issues() -> None:
    log_list = [
        "20:30:01  \x1b[33mRunning with dbt=1.3.0\x1b[0m",
        "20:30:03  \x1b[33mFinished running 1 test in 10.31s.\x1b[0m",
        "20:30:02  \x1b[33mWarning in test my_test (models/my_model.sql)\x1b[0m",
        "20:30:02  \x1b[33mSome warning message\x1b[0m",
        "20:30:03  \x1b[33mWarning in test my_second_test (models/my_model.sql)\x1b[0m",
        "20:30:03  \x1b[33mA very different warning message\x1b[0m",
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert "my_test" in test_names
    assert "my_second_test" in test_names
    assert "Some warning message" in test_results
    assert "A very different warning message" in test_results

    log_list_no_warning = [
        "20:30:01  \x1b[33mRunning with dbt=1.3.0\x1b[0m",
        "20:30:03  \x1b[33mFinished running 1 test in 10.31s.\x1b[0m",
    ]
    test_names_no_warns, test_results_no_warns = extract_log_issues(log_list_no_warning)
    assert test_names_no_warns == []
    assert test_results_no_warns == []


def test_extract_log_issues_dbt_1_12_format() -> None:
    """dbt 1.12 reworded "Warning in test <name> (<path>)" to "[WARNING]: in test <name>
    (<path>)", and the result line gained the same "[WARNING]: " prefix. Regression for the
    dbt-1.12 CI failure where on_warning_callback fired with empty test_names/test_results."""
    log_list = [
        "11:17:43  Finished running 1 test in 0 hours 0 minutes and 0.13 seconds (0.13s).",
        "11:17:43  \x1b[33mCompleted with 1 warning:\x1b[0m",
        "11:17:43  [\x1b[33mWARNING\x1b[0m]: in test accepted_values_mini_orders_status__placed (models/schema.yml)",
        "11:17:43  [\x1b[33mWARNING\x1b[0m]: Got 4 results, configured to warn if >1",
        "11:17:43  Done. PASS=0 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1",
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["accepted_values_mini_orders_status__placed"]
    assert test_results == ["Got 4 results, configured to warn if >1"]


def test_extract_dbt_runner_issues():
    """Tests that the function extracts the correct node names and messages from a dbt runner result
    for warnings by default.
    """
    runner_result = MagicMock()
    runner_result.result.results = [
        MagicMock(status="pass"),
        MagicMock(status="warn", message="A warning message", node=MagicMock()),
        MagicMock(status="pass"),
        MagicMock(status="warn", message="A different warning message", node=MagicMock()),
    ]
    runner_result.result.results[1].node.name = "a_test"
    runner_result.result.results[3].node.name = "another_test"

    node_names, node_results = extract_dbt_runner_issues(runner_result)

    assert node_names == ["a_test", "another_test"]
    assert node_results == ["A warning message", "A different warning message"]


def test_extract_dbt_runner_issues_with_status_levels():
    """Tests that the function extracts the correct test names and results from a dbt runner result
    for status levels.
    """
    runner_result = MagicMock()
    runner_result.result.results = [
        MagicMock(status="pass"),
        MagicMock(status="error", message="An error message", node=MagicMock()),
        MagicMock(status="warn"),
        MagicMock(status="fail", message="A failure message", node=MagicMock()),
    ]
    runner_result.result.results[1].node.name = "node1"
    runner_result.result.results[3].node.name = "node2"

    node_names, node_results = extract_dbt_runner_issues(runner_result, status_levels=["error", "fail"])

    assert node_names == ["node1", "node2"]
    assert node_results == ["An error message", "A failure message"]


def test_extract_freshness_warn_msg():
    result = FullOutputSubprocessResult(
        full_output=[
            "Info: some other log message",
            "INFO - 11:50:42  1 of 1 WARN freshness of postgres_db.raw_orders ................................ [WARN in 0.01s]",
            "INFO - 11:50:42",
            "INFO - 11:50:42  Finished running 1 source in 0 hours 0 minutes and 0.04 seconds (0.04s).",
            "INFO - 11:50:42  Done.",
        ],
        output="INFO - 11:50:42  Done.",
        exit_code=0,
    )
    node_names, node_results = extract_freshness_warn_msg(result)

    assert node_names == ["postgres_db.raw_orders"]
    assert node_results == [
        "INFO - 11:50:42  1 of 1 WARN freshness of postgres_db.raw_orders ................................ [WARN in 0.01s]"
    ]
