import logging
import re


def parse_output(output: str, keyword: str) -> int:
    """
    Parses the DBT test output message and returns the number of errors or warnings.

    :param output: String containing the output to be parsed.
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

    try:
        num = int(output.split(f"{keyword}=")[1].split()[0])
    except ValueError:
        logging.error(
            f"Could not parse number of {keyword}s. Please, check your DBT or Airflow version"
        )
        num = 0
    return num


def extract_log_issues(log_list: list[str]) -> str:
    """
    Extracts warning messages from the log list and returns them as a formatted string.

    This function searches for warning messages in DBT test. It reverses the log list for performance
    improvement.It extracts and formats the relevant information and appends it to a list of warnings.

    :param log_list: List of strings, where each string is a log line from DBT test.
    :param key: String containing the key to search for in the log list. it can be "Warning" or "Error".
    :return: A string containing the formatted warning messages

    Usage:
    -----
    log_lines = [
        "12:30:00 Warning in test example_test",
        "12:30:01 Got 10 results, configured to warn if <15",
        "12:30:02 Finished running",
        "12:30:03 Another log line"
    ]
    extracted_warnings = extract_log_issues(log_lines)
    print(extracted_warnings)
    # Output:
    # *Warning in test example_test*: Got 10 results, configured to warn if <15
    """
    test_names = []
    test_results = []
    for i, line in enumerate(reversed(log_list)):
        cleaned_line = line.replace("\x1b[33m", "").replace("\x1b[0m", "").strip()

        if "Finished running" in cleaned_line:
            # No need to keep checking the log lines
            break

        if "Warning in test" in cleaned_line:
            pattern1 = r"\d{2}:\d{2}:\d{2}\s+Warning in test ([\w_]+).*"
            test_name = cleaned_line
            test_name = re.sub(pattern1, r"\1", test_name).strip()
            test_result = (
                log_list[-(i + 1) + 1].replace("\x1b[33m", "").replace("\x1b[0m", "")
            )
            pattern2 = (
                r"\d{2}:\d{2}:\d{2}\s+(Got \d+ results, configured to warn if .+)"
            )
            test_result = re.sub(pattern2, r"\1", test_result).strip()
            test_names.append(test_name)
            test_results.append(test_result)
    return test_names, test_results
