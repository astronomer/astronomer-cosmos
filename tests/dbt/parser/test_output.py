import json
from unittest.mock import MagicMock

import pytest

from cosmos.dbt.parser.output import (
    extract_dbt_failure_details,
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
        # dbt 2 (dbt-oss) leaves out the categories that are 0.
        (["Summary: 1 total | 1 warn"], 1),
        (["Summary: 4 total | 1 success | 1 warn | 2 error"], 1),
        (["Summary: 3 total | 3 warn"], 3),
        (["Summary: 1 total | 1 success"], 0),
        (["Summary: 1 total | 1 error"], 0),
        # Colours forced with CLICOLOR_FORCE=1.
        (
            ["Summary: \x1b[37m\x1b[1m9 total\x1b[0m | \x1b[32m\x1b[1m6 success\x1b[0m | \x1b[33m\x1b[1m3 warn\x1b[0m"],
            3,
        ),
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


@pytest.mark.parametrize(
    "finished_running",
    [
        "11:17:43  Finished running 1 test in 0 hours 0 minutes and 0.13 seconds (0.13s).",
        "11:17:43  Finished running 10 data tests, 3 unit tests in 0 hours 0 minutes and 0.78 seconds (0.78s).",
    ],
)
def test_extract_log_issues_dbt_1_ignores_warnings_above_finished_running(finished_running: str) -> None:
    """Only the warnings after ``Finished running N tests`` belong to the test run; older ones are excluded."""
    log_list = [
        "11:17:40  Warning in test older_test (models/schema.yml)",
        "11:17:40  Got 2 results, configured to warn if >1",
        finished_running,
        "11:17:43  Warning in test new_test (models/schema.yml)",
        "11:17:43  Got 4 results, configured to warn if >1",
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["new_test"]
    assert test_results == ["Got 4 results, configured to warn if >1"]


def test_extract_log_issues_dbt_v2_namespaced_test_name() -> None:
    log_list = ["    Warned test  main_dbt_test__audit.sing_warn (tests/sing_warn.sql) [1 of 4 in 1.0s]"]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["main_dbt_test__audit.sing_warn"]
    assert test_results == ["Warned test main_dbt_test__audit.sing_warn (tests/sing_warn.sql)"]


def _json_log_line(msg: str) -> str:
    """A dbt ``--log-format json`` line."""
    return json.dumps({"data": {}, "info": {"level": "info", "msg": msg, "ts": "2026-10-19T10:00:00.000000Z"}})


def test_extract_log_issues_json_log_format() -> None:
    log_list = [
        _json_log_line("Finished running 2 data tests, 1 view model in 0 hours 0 minutes and 0.24 seconds (0.24s)."),
        _json_log_line("\x1b[33mWarning in test unique_m_id (models/schema.yml)\x1b[0m"),
        _json_log_line("Got 1 result, configured to warn if != 0"),
        _json_log_line("Done. PASS=1 WARN=1 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2"),
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["unique_m_id"]
    assert test_results == ["Got 1 result, configured to warn if != 0"]


def test_extract_log_issues_json_log_format_dbt_1_12() -> None:
    log_list = [
        _json_log_line("Finished running 1 test in 0 hours 0 minutes and 0.13 seconds (0.13s)."),
        _json_log_line("[WARNING]: in test accepted_values_mini_orders_status__placed (models/schema.yml)"),
        _json_log_line("[WARNING]: Got 4 results, configured to warn if >1"),
        _json_log_line("Done. PASS=0 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=1"),
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["accepted_values_mini_orders_status__placed"]
    assert test_results == ["Got 4 results, configured to warn if >1"]


def test_extract_dbt_failure_details() -> None:
    log_list = [
        "20:30:01  \x1b[31mRunning with dbt=1.3.0\x1b[0m",
        "20:30:02  \x1b[31mFailure in test not_null_my_model_id (models/schema.yml)\x1b[0m",
        "20:30:02  \x1b[31m  Got 5 results, configured to fail if != 0\x1b[0m",
        "20:30:03  \x1b[31mError in model my_model (models/my_model.sql)\x1b[0m",
        "20:30:03  \x1b[31m  Database Error: relation does not exist\x1b[0m",
    ]
    details = extract_dbt_failure_details(log_list)
    assert details == [
        "Failure in test not_null_my_model_id (models/schema.yml) Got 5 results, configured to fail if != 0",
        "Error in model my_model (models/my_model.sql) Database Error: relation does not exist",
    ]

    assert extract_dbt_failure_details(["20:30:01  Running with dbt=1.3.0", "20:30:02  Completed successfully"]) == []


def test_extract_dbt_failure_details_json_log_format() -> None:
    log_list = [
        _json_log_line("\x1b[31mFailure in test not_null_m_id (models/schema.yml)\x1b[0m"),
        _json_log_line("  Got 1 result, configured to fail if != 0"),
        "not a json line",
    ]
    assert extract_dbt_failure_details(log_list) == [
        "Failure in test not_null_m_id (models/schema.yml) Got 1 result, configured to fail if != 0"
    ]


def test_extract_dbt_failure_details_json_log_format_dbt_1_12() -> None:
    log_list = [
        _json_log_line("[ERROR]: in test not_null_m_id (models/schema.yml)"),
        _json_log_line("[ERROR]: Got 1 result, configured to fail if != 0"),
    ]
    assert extract_dbt_failure_details(log_list) == [
        "Error in test not_null_m_id (models/schema.yml) Got 1 result, configured to fail if != 0"
    ]


# The dbt 2 lines below were captured from dbt-oss 2.0.8 running ``dbt test`` on tests/sample/mini and a variant with
# several tests. dbt 2 writes its text output to stderr, which the subprocess hook merges into the output.
DBT_V2_WARNED_TESTS_LOG = [
    "   dbt-oss 2.0.8",
    "   Loading profiles.yml",
    "    Failed test  unique_mini_orders_id (models/schema.yml:16:13) [1 of 4 in 1.02s]",
    "    Passed test  not_null_mini_orders_status [2 of 4 in 1.02s]",
    "    Warned test  accepted_values_mini_orders_status__placed (models/schema.yml:7:13) [3 of 4 in 2.03s]",
    "    Warned test  other_warned_test (models/schema.yml:20:13) [4 of 4 in 2.04s]",
    "==================== Execution Summary =====================",
    "Finished 'test' with 2 warnings and 1 error for target 'dev' [2.3s]",
    "Processed: 4 tests",
    "Summary: 4 total | 1 success | 2 warn | 1 error",
]


def test_extract_log_issues_dbt_v2() -> None:
    test_names, test_results = extract_log_issues(DBT_V2_WARNED_TESTS_LOG)
    assert sorted(test_names) == ["accepted_values_mini_orders_status__placed", "other_warned_test"]
    assert "Warned test accepted_values_mini_orders_status__placed (models/schema.yml:7:13)" in test_results
    assert len(test_results) == 2


def test_extract_log_issues_dbt_v2_debug_does_not_stop_at_the_first_finished_running_line() -> None:
    log_list = [
        "Finished running [  0.01s] test main_dbt_test__audit.first_test [warn]",
        "    Warned test  first_test (models/schema.yml:7:13) [1 of 2 in 0.01s]",
        "Finished running [  0.01s] test main_dbt_test__audit.second_test [warn]",
        "    Warned test  second_test (models/schema.yml:9:13) [2 of 2 in 0.01s]",
        "Summary: 2 total | 2 warn",
    ]
    test_names, _ = extract_log_issues(log_list)
    assert sorted(test_names) == ["first_test", "second_test"]


def test_extract_log_issues_dbt_v2_json_log_format_with_node_finished_event() -> None:
    log_list = [
        _json_log_line("    Warned test  my_test (models/schema.yml:13:13) [1 of 1 in 0.01s]"),
        _json_log_line("Finished running node test.mini.my_test.c968fa614a"),
        _json_log_line("\n=== Execution Summary ===\nSummary: 1 total | 1 warn"),
    ]
    assert extract_log_issues(log_list)[0] == ["my_test"]


def test_extract_log_issues_dbt_v2_no_warning() -> None:
    log_list = ["    Passed test  not_null_mini_orders_status [1 of 1 in 0.01s]", "Summary: 1 total | 1 success"]
    assert extract_log_issues(log_list) == ([], [])


def test_extract_log_issues_dbt_v2_json_log_format() -> None:
    log_list = [
        _json_log_line("    Warned test  my_test (models/schema.yml:13:13) [1 of 1 in 0.01s]"),
        _json_log_line("\n=== Execution Summary ===\nSummary: 1 total | 1 warn"),
    ]
    test_names, test_results = extract_log_issues(log_list)
    assert test_names == ["my_test"]
    assert test_results == ["Warned test my_test (models/schema.yml:13:13)"]


def test_extract_dbt_failure_details_dbt_v2() -> None:
    log_list = [
        "   dbt-oss 2.0.8",
        "    Failed test  accepted_values_mini_orders_status__placed (models/schema.yml:13:13) [1 of 1 in 0.01s]",
        "====================== Test Failures =======================",
        "Test failed (4 failed row(s)): accepted_values_mini_orders_status__placed",
        "==================== Execution Summary =====================",
        "Summary: 1 total | 1 error",
    ]
    assert extract_dbt_failure_details(log_list) == [
        "Failure in test accepted_values_mini_orders_status__placed (models/schema.yml:13:13) 4 failed row(s)"
    ]


def test_extract_dbt_failure_details_dbt_v2_test_that_errored_has_no_row_count() -> None:
    log_list = ["    Failed test  unique_mini_orders_id (models/schema.yml:16:13) [1 of 4 in 1.02s]"]
    assert extract_dbt_failure_details(log_list) == ["Failure in test unique_mini_orders_id (models/schema.yml:16:13)"]


def test_extract_dbt_failure_details_dbt_v2_namespaced_test_name() -> None:
    log_list = [
        "    Failed test  main_dbt_test__audit.sing_fail (tests/sing_fail.sql) [1 of 1 in 0.01s]",
        "Test failed (1 failed row(s)): sing_fail",
    ]
    assert extract_dbt_failure_details(log_list) == [
        "Failure in test main_dbt_test__audit.sing_fail (tests/sing_fail.sql) 1 failed row(s)"
    ]


def test_extract_dbt_failure_details_dbt_v2_json_log_format() -> None:
    # In JSON output dbt 2 has the "Failed test" line only, not the "Test failed (N failed row(s))" one.
    log_list = [_json_log_line("    Failed test  not_null_m_id (models/schema.yml:3:9) [1 of 1 in 0.01s]")]
    assert extract_dbt_failure_details(log_list) == ["Failure in test not_null_m_id (models/schema.yml:3:9)"]


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


def test_extract_freshness_warn_msg_json_log_format():
    warn_msg = "1 of 1 WARN freshness of postgres_db.raw_orders ................................ [WARN in 0.01s]"
    result = FullOutputSubprocessResult(
        full_output=[_json_log_line(warn_msg), _json_log_line("Done.")],
        output=_json_log_line("Done."),
        exit_code=0,
    )
    node_names, node_results = extract_freshness_warn_msg(result)

    assert node_names == ["postgres_db.raw_orders"]
    assert node_results == [warn_msg]


# What dbt 2 (dbt-oss 2.0.8) prints for ``dbt source freshness`` when raw_orders is past ``warn_after`` and raw_orders_stale
# is past ``error_after``.
DBT_V2_FRESHNESS_OUTPUT = [
    "   dbt-oss 2.0.8",
    "   Loading profiles.yml",
    "    Warned source raw.raw_orders (last updated 8years 9months 26days 11h 44m 36s ago) [0.01s]",
    "     Stale source raw.raw_orders_stale (last updated 8years 9months 26days 11h 44m 36s ago) [0.01s]",
    "Successfully wrote sources.json",
    "=================== Errors and Warnings ====================",
    "[warning] [StaleSource (dbt1063)]: Stale source source.jaffle_shop.raw.raw_orders",
    "  --> models/staging/sources.yml",
    "[error] [StaleSource (dbt1063)]: Stale source source.jaffle_shop.raw.raw_orders_stale",
    "  --> models/staging/sources.yml",
    "==================== Execution Summary =====================",
    "Finished 'freshness' with 3 warnings and 1 error for target 'dev' [646ms]",
]


def test_extract_freshness_warn_msg_dbt_v2():
    """dbt 2 prints ``Warned source`` for a warning: the stale (error) source and the diagnostics are not warnings."""
    result = FullOutputSubprocessResult(
        full_output=DBT_V2_FRESHNESS_OUTPUT, output=DBT_V2_FRESHNESS_OUTPUT[-1], exit_code=1
    )
    node_names, node_results = extract_freshness_warn_msg(result)

    assert node_names == ["raw.raw_orders"]
    assert node_results == ["Warned source raw.raw_orders (last updated 8years 9months 26days 11h 44m 36s ago) [0.01s]"]


def test_extract_freshness_warn_msg_dbt_v2_ignores_colors_and_timestamps():
    lines = [
        "\x1b[33m    Warned source raw.raw_orders (last updated 1day ago) [0.01s]\x1b[0m",
        "10:46:31  Warned source raw.raw_payments (last updated 2days ago) [0.02s]",
    ]
    result = FullOutputSubprocessResult(full_output=lines, output=lines[-1], exit_code=0)
    node_names, node_results = extract_freshness_warn_msg(result)

    assert node_names == ["raw.raw_orders", "raw.raw_payments"]
    assert node_results == [
        "Warned source raw.raw_orders (last updated 1day ago) [0.01s]",
        "10:46:31  Warned source raw.raw_payments (last updated 2days ago) [0.02s]",
    ]


def test_extract_freshness_warn_msg_dbt_v2_without_warnings():
    lines = ["   dbt-oss 2.0.8", "     Passed source raw.raw_orders (last updated 1h ago) [0.01s]"]
    result = FullOutputSubprocessResult(full_output=lines, output=lines[-1], exit_code=0)

    assert extract_freshness_warn_msg(result) == ([], [])
