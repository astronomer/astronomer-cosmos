import logging
import re
from typing import List, Tuple

from airflow.hooks.subprocess import SubprocessResult


def parse_output(result: SubprocessResult, keyword: str) -> int:
    """
    Parses the dbt test output message and returns the number of errors or warnings.

    :param result: String containing the output to be parsed.
    :param keyword: String representing the keyword to search for in the output (WARN, ERROR).
    :return: An integer value associated with the keyword, or 0 if parsing fails.

    Usage:
    -----
    output_str = "Done. PASS=15 WARN=1 ERROR=0 SKIP=0 TOTAL=16"
    keyword = "WARN"
    num_warns = parse_output(output_str, keyword)
    print(num_warns)
    # Output: 1
    """
    output = result.output
    try:
        num = int(output.split(f"{keyword}=")[1].split()[0])
    except ValueError:
        logging.error(
            f"Could not parse number of {keyword}s. Check your dbt/airflow version or if --quiet is not being used"
        )
    return num


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
