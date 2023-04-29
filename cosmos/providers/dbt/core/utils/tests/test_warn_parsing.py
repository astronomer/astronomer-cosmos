from airflow.hooks.subprocess import SubprocessResult

from cosmos.providers.dbt.core.utils.warn_parsing import (
    extract_log_issues,
    parse_output,
)


def test_parse_output() -> None:
    for warnings in range(0, 3):
        output_str = f"Done. PASS=15 WARN={warnings} ERROR=0 SKIP=0 TOTAL=16"
        keyword = "WARN"
        result = SubprocessResult(exit_code=0, output=output_str)
        num_warns = parse_output(result, keyword)
        assert num_warns == warnings


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
