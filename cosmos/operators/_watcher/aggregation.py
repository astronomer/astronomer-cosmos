from __future__ import annotations

from typing import Any

from cosmos.log import get_logger
from cosmos.operators._watcher.state import DbtTestStatus, safe_xcom_push

logger = get_logger(__name__)


def get_tests_status_xcom_key(model_uid: str) -> str:
    """Return the XCom key used to store the aggregated test status for a model."""
    return f"{model_uid.replace('.', '__')}_tests_status"


def accumulate_test_result(
    test_unique_id: str,
    status: str,
    tests_per_model: dict[str, list[str]],
    test_results_per_model: dict[str, list[str]],
) -> str | None:
    """Accumulate a test's terminal status into test_results_per_model for its parent model.

    Returns the parent model's unique_id if found, else None.
    """
    for model_uid, test_uids in tests_per_model.items():
        if test_unique_id in test_uids:
            test_results_per_model.setdefault(model_uid, []).append(status)
            return model_uid
    return None


def get_aggregated_test_status(
    model_uid: str,
    tests_per_model: dict[str, list[str]],
    test_results_per_model: dict[str, list[str]],
) -> str | None:
    """
    Check if all tests for a model have finished and return aggregated status.

    Returns:
        "pass" if all tests passed, "fail" if any test failed,
        or None if not all tests have reported yet.
    """
    expected = tests_per_model.get(model_uid)
    if not expected:
        return None
    collected = test_results_per_model.get(model_uid, [])
    if len(collected) < len(expected):
        logger.debug(
            "Model '%s' has %s tests, but only %s have reported results so far.",
            model_uid,
            len(expected),
            len(collected),
        )
        return None
    aggregated_test_result = (
        DbtTestStatus.PASS if all(s == DbtTestStatus.PASS for s in collected) else DbtTestStatus.FAIL
    )
    logger.debug("Model '%s' has all tests reported. Aggregated result: %s", model_uid, aggregated_test_result)
    return aggregated_test_result


def push_test_result_or_aggregate(
    test_unique_id: str,
    status: str,
    tests_per_model: dict[str, list[str]],
    test_results_per_model: dict[str, list[str]],
    task_instance: Any,
) -> None:
    """Accumulate a test result and, when all tests for the parent model have reported, push aggregated XCom.

    :param test_unique_id: The unique_id of the finished test node.
    :param status: The terminal status of the test (e.g. "pass", "fail").
    :param tests_per_model: Mapping of model unique_id → list of test unique_ids.
    :param test_results_per_model: Mutable accumulator, mutated in place.
    :param task_instance: The Airflow task instance used for XCom push.
    """
    model_uid = accumulate_test_result(test_unique_id, status, tests_per_model, test_results_per_model)
    if model_uid is not None:
        aggregated = get_aggregated_test_status(model_uid, tests_per_model, test_results_per_model)
        if aggregated is not None:
            safe_xcom_push(
                task_instance=task_instance,
                key=get_tests_status_xcom_key(model_uid),
                value=aggregated,
            )
