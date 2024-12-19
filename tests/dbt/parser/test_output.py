import logging
from unittest.mock import MagicMock

import pytest
from airflow.hooks.subprocess import SubprocessResult

from cosmos.dbt.parser.output import (
    extract_dbt_runner_issues,
    extract_freshness_warn_msg,
    extract_log_issues,
    parse_number_of_warnings_dbt_runner,
    parse_number_of_warnings_subprocess,
)
from cosmos.hooks.subprocess import FullOutputSubprocessResult


@pytest.mark.parametrize(
    "output_str, expected_warnings",
    [
        ("Done. PASS=15 WARN=1 ERROR=0 SKIP=0 TOTAL=16", 1),
        ("Done. PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=16", 0),
        ("Done. PASS=15 WARN=2 ERROR=0 SKIP=0 TOTAL=16", 2),
        ("Nothing to do. Exiting without running tests.", 0),
    ],
)
def test_parse_number_of_warnings_subprocess(output_str: str, expected_warnings):
    result = SubprocessResult(exit_code=0, output=output_str)
    num_warns = parse_number_of_warnings_subprocess(result)
    assert num_warns == expected_warnings


def test_parse_number_of_warnings_subprocess_error_logged(caplog):
    output_str = "WARN= should log an error."
    with caplog.at_level(logging.ERROR):
        result = SubprocessResult(exit_code=0, output=output_str)
        parse_number_of_warnings_subprocess(result)
    expected_error_log = (
        "Could not parse number of WARNs. Check your dbt/airflow version or if --quiet is not being used"
    )
    assert expected_error_log in caplog.text


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
