from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any, TypeAlias

from cosmos.log import get_logger
from cosmos.operators._watcher.state import DbtTestStatus, is_dbt_node_status_success, safe_xcom_push

logger = get_logger(__name__)

# Protects all mutations of ``test_results_per_model`` so that concurrent
# dbt threads cannot interleave ``setdefault`` / item assignment / ``len`` checks.
_test_results_lock = Lock()

#: Maximum number of failed test IDs included in the summary.
#: Keeps XCom/event payloads bounded for models with many tests.
MAX_FAILED_TESTS_IN_SUMMARY = 5


#: Mapping of model unique_id → list of test unique_ids associated with that model.
TestsPerModel: TypeAlias = dict[str, list[str]]

#: Mutable accumulator mapping model unique_id → {test unique_id → terminal status}.
#: Keying by test unique_id makes duplicated/replayed log lines idempotent.
ResultsTestsPerModel: TypeAlias = dict[str, dict[str, str]]


@dataclass(frozen=True)
class TestResultSummary:
    """Aggregated test result for a model, pushed as XCom and forwarded in TriggerEvents."""

    # Prevent pytest from collecting this class as a test suite.
    __test__ = False

    status: DbtTestStatus
    passed_count: int
    failed_count: int
    total_count: int
    failed_tests: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TestResultSummary:
        status = data.get("status")
        if status not in (DbtTestStatus.PASS, DbtTestStatus.FAIL):
            raise ValueError(f"Invalid TestResultSummary status: {status!r}")
        return cls(
            status=status,
            passed_count=data.get("passed_count", 0),
            failed_count=data.get("failed_count", 0),
            total_count=data.get("total_count", 0),
            failed_tests=data.get("failed_tests", []),
        )


def get_tests_status_xcom_key(model_uid: str) -> str:
    """Return the XCom key used to store the aggregated test status for a model."""
    return f"{model_uid.replace('.', '__')}_tests_status"


def accumulate_test_result(
    test_unique_id: str,
    status: str,
    tests_per_model: TestsPerModel,
    test_results_per_model: ResultsTestsPerModel,
) -> str | None:
    """Record a test's terminal status into test_results_per_model for its parent model.

    Results are keyed by ``test_unique_id`` so that a duplicated or replayed log line for
    the same test (the Kubernetes log reader can replay recent lines after an interruption)
    updates its entry instead of being counted as another distinct test. When a test is
    reported more than once, the worst (most severe) status is kept: a later failure
    overrides an earlier pass, but a later pass never overrides an earlier failure.

    Returns the parent model's unique_id if found, else None.
    """
    for model_uid, test_uids in tests_per_model.items():
        if test_unique_id in test_uids:
            collected = test_results_per_model.setdefault(model_uid, {})
            prev_status = collected.get(test_unique_id)
            # Keep the worst (most severe) status per test. A later failure
            # overrides a previous pass, but a later pass does NOT override a failure.
            if prev_status is None or (
                not is_dbt_node_status_success(status) and is_dbt_node_status_success(prev_status)
            ):
                collected[test_unique_id] = status
            return model_uid
    return None


def get_aggregated_test_status(
    model_uid: str,
    tests_per_model: TestsPerModel,
    test_results_per_model: ResultsTestsPerModel,
) -> TestResultSummary | None:
    """
    Check if all tests for a model have finished and return aggregated status.

    Returns:
        A TestResultSummary with pass/fail breakdown if all tests have reported,
        or None if not all tests have reported yet.
    """
    expected = tests_per_model.get(model_uid)
    if not expected:
        return None
    collected = test_results_per_model.get(model_uid, {})
    if len(collected) < len(expected):
        logger.debug(
            "Model '%s' has %s tests, but only %s have reported results so far.",
            model_uid,
            len(expected),
            len(collected),
        )
        return None
    passed_count = sum(1 for s in collected.values() if is_dbt_node_status_success(s))
    failed_tests = [tid for tid, s in collected.items() if not is_dbt_node_status_success(s)]
    aggregated_status = DbtTestStatus.PASS if not failed_tests else DbtTestStatus.FAIL
    logger.debug("Model '%s' has all tests reported. Aggregated result: %s", model_uid, aggregated_status)
    return TestResultSummary(
        status=aggregated_status,
        passed_count=passed_count,
        failed_count=len(failed_tests),
        total_count=len(collected),
        failed_tests=failed_tests[:MAX_FAILED_TESTS_IN_SUMMARY],
    )


def push_test_result_or_aggregate(
    test_unique_id: str,
    status: str,
    tests_per_model: TestsPerModel,
    test_results_per_model: ResultsTestsPerModel,
    task_instance: Any,
) -> None:
    """Accumulate a test result and, when all tests for the parent model have reported, push aggregated XCom.

    :param test_unique_id: The unique_id of the finished test node.
    :param status: The terminal status of the test (e.g. "pass", "fail").
    :param tests_per_model: Mapping of model unique_id → list of test unique_ids.
    :param test_results_per_model: Mutable accumulator, mutated in place.
    :param task_instance: The Airflow task instance used for XCom push.
    """
    with _test_results_lock:
        model_uid = accumulate_test_result(test_unique_id, status, tests_per_model, test_results_per_model)
        if model_uid is not None:
            aggregated = get_aggregated_test_status(model_uid, tests_per_model, test_results_per_model)
            if aggregated is not None:
                safe_xcom_push(
                    task_instance=task_instance,
                    key=get_tests_status_xcom_key(model_uid),
                    value=aggregated.to_dict(),
                )
