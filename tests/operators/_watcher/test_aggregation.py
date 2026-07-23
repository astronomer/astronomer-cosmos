"""Unit tests for cosmos.operators._watcher.aggregation module."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from cosmos.operators._watcher.aggregation import (
    ResultsTestsPerModel,
    TestResultSummary,
    accumulate_test_result,
    get_aggregated_test_status,
    get_tests_status_xcom_key,
    push_test_result_or_aggregate,
)
from cosmos.operators._watcher.state import DbtTestStatus

TESTS_PER_MODEL = {
    "model.pkg.orders": ["test.pkg.not_null_orders_id", "test.pkg.unique_orders_id"],
    "model.pkg.customers": ["test.pkg.not_null_customers_id"],
}


class TestGetTestsStatusXcomKey:
    """Tests for get_tests_status_xcom_key."""

    def test_replaces_dots_with_double_underscores(self):
        assert get_tests_status_xcom_key("model.pkg.orders") == "model__pkg__orders_tests_status"

    def test_no_dots(self):
        assert get_tests_status_xcom_key("orders") == "orders_tests_status"


class TestTestResultSummaryClass:
    """Tests for TestResultSummary dataclass."""

    def test_to_dict_roundtrip(self):
        summary = TestResultSummary(
            status=DbtTestStatus.PASS,
            passed_count=2,
            failed_count=0,
            total_count=2,
            failed_tests=[],
        )
        d = summary.to_dict()
        assert d == {
            "status": "pass",
            "passed_count": 2,
            "failed_count": 0,
            "total_count": 2,
            "failed_tests": [],
        }
        restored = TestResultSummary.from_dict(d)
        assert restored == summary

    def test_from_dict_with_fail_status(self):
        d = {
            "status": "fail",
            "passed_count": 1,
            "failed_count": 1,
            "total_count": 2,
            "failed_tests": ["test.pkg.b"],
        }
        summary = TestResultSummary.from_dict(d)
        assert summary.status == DbtTestStatus.FAIL
        assert summary.failed_tests == ["test.pkg.b"]

    def test_from_dict_raises_on_invalid_status(self):
        with pytest.raises(ValueError, match="Invalid TestResultSummary status"):
            TestResultSummary.from_dict({"status": "unknown"})

    def test_from_dict_raises_on_missing_status(self):
        with pytest.raises(ValueError, match="Invalid TestResultSummary status"):
            TestResultSummary.from_dict({})

    def test_frozen_dataclass_is_immutable(self):
        summary = TestResultSummary(
            status=DbtTestStatus.PASS,
            passed_count=1,
            failed_count=0,
            total_count=1,
            failed_tests=[],
        )
        with pytest.raises(AttributeError):
            summary.status = DbtTestStatus.FAIL  # type: ignore[misc]


class TestAccumulateTestResult:
    """Tests for accumulate_test_result."""

    def test_returns_model_uid_when_test_found(self):
        results: ResultsTestsPerModel = {}
        model_uid = accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        assert model_uid == "model.pkg.orders"
        assert results == {"model.pkg.orders": {"test.pkg.not_null_orders_id": "pass"}}

    def test_returns_none_when_test_not_found(self):
        results: ResultsTestsPerModel = {}
        model_uid = accumulate_test_result("test.pkg.unknown_test", "pass", TESTS_PER_MODEL, results)
        assert model_uid is None
        assert results == {}

    def test_accumulates_multiple_results(self):
        results: ResultsTestsPerModel = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.unique_orders_id", "fail", TESTS_PER_MODEL, results)
        assert results == {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "pass", "test.pkg.unique_orders_id": "fail"}
        }

    def test_accumulates_across_models(self):
        results: ResultsTestsPerModel = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.not_null_customers_id", "pass", TESTS_PER_MODEL, results)
        assert results == {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "pass"},
            "model.pkg.customers": {"test.pkg.not_null_customers_id": "pass"},
        }

    def test_deduplicates_same_test_keeps_worst_status(self):
        results: ResultsTestsPerModel = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.not_null_orders_id", "fail", TESTS_PER_MODEL, results)
        # The worse (fail) status should override the earlier pass
        assert results == {"model.pkg.orders": {"test.pkg.not_null_orders_id": "fail"}}

    def test_deduplicates_same_test_keeps_first_failure(self):
        results: ResultsTestsPerModel = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "fail", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        # A later pass should NOT override an existing failure
        assert results == {"model.pkg.orders": {"test.pkg.not_null_orders_id": "fail"}}


class TestGetAggregatedTestStatus:
    """Tests for get_aggregated_test_status."""

    def test_returns_none_when_model_not_in_tests_per_model(self):
        assert get_aggregated_test_status("model.pkg.unknown", TESTS_PER_MODEL, {}) is None

    def test_returns_none_when_not_all_tests_reported(self):
        results: ResultsTestsPerModel = {"model.pkg.orders": {"test.pkg.not_null_orders_id": "pass"}}
        assert get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results) is None

    def test_returns_summary_pass_when_all_tests_pass(self):
        results: ResultsTestsPerModel = {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "pass", "test.pkg.unique_orders_id": "pass"}
        }
        summary = get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.PASS
        assert summary.passed_count == 2
        assert summary.failed_count == 0
        assert summary.total_count == 2
        assert summary.failed_tests == []

    def test_returns_summary_fail_when_any_test_fails(self):
        results: ResultsTestsPerModel = {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "pass", "test.pkg.unique_orders_id": "fail"}
        }
        summary = get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.FAIL
        assert summary.passed_count == 1
        assert summary.failed_count == 1
        assert summary.failed_tests == ["test.pkg.unique_orders_id"]

    def test_returns_summary_fail_when_all_tests_fail(self):
        results: ResultsTestsPerModel = {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "fail", "test.pkg.unique_orders_id": "fail"}
        }
        summary = get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.FAIL
        assert summary.failed_count == 2

    def test_single_test_pass(self):
        results: ResultsTestsPerModel = {"model.pkg.customers": {"test.pkg.not_null_customers_id": "pass"}}
        summary = get_aggregated_test_status("model.pkg.customers", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.PASS
        assert summary.total_count == 1

    def test_single_test_fail(self):
        results: ResultsTestsPerModel = {"model.pkg.customers": {"test.pkg.not_null_customers_id": "fail"}}
        summary = get_aggregated_test_status("model.pkg.customers", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.FAIL

    def test_returns_fail_when_test_has_error_status(self):
        results: ResultsTestsPerModel = {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "pass", "test.pkg.unique_orders_id": "error"}
        }
        summary = get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.FAIL
        assert summary.failed_tests == ["test.pkg.unique_orders_id"]

    def test_treats_success_status_as_pass(self):
        """'success' is used by models; tests use 'pass' — both should be treated as success."""
        results: ResultsTestsPerModel = {
            "model.pkg.orders": {"test.pkg.not_null_orders_id": "success", "test.pkg.unique_orders_id": "pass"}
        }
        summary = get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results)
        assert summary is not None
        assert summary.status == DbtTestStatus.PASS


class TestPushTestResultOrAggregate:
    """Tests for push_test_result_or_aggregate."""

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_pushes_xcom_dict_when_all_tests_reported(self, mock_xcom_push: MagicMock):
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.unique_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_called_once()
        call_kwargs = mock_xcom_push.call_args[1]
        assert call_kwargs["key"] == "model__pkg__orders_tests_status"
        value = call_kwargs["value"]
        assert isinstance(value, dict)
        assert value["status"] == DbtTestStatus.PASS
        assert value["passed_count"] == 2
        assert value["failed_count"] == 0

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_does_not_push_xcom_before_all_tests_reported(self, mock_xcom_push: MagicMock):
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_not_called()

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_does_not_push_xcom_for_unknown_test(self, mock_xcom_push: MagicMock):
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.unknown", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_not_called()

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_pushes_fail_dict_when_any_test_fails(self, mock_xcom_push: MagicMock):
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.unique_orders_id", "fail", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_called_once()
        value = mock_xcom_push.call_args[1]["value"]
        assert value["status"] == DbtTestStatus.FAIL
        assert value["passed_count"] == 1
        assert value["failed_count"] == 1
        assert value["failed_tests"] == ["test.pkg.unique_orders_id"]

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_deduplication_prevents_double_push(self, mock_xcom_push: MagicMock):
        """If a test result is reported twice, it should not count twice toward completion."""
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        # Still only 1 result recorded, not all tests reported yet
        mock_xcom_push.assert_not_called()

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_replayed_test_result_does_not_prematurely_aggregate(self, mock_xcom_push: MagicMock):
        """A replayed log line for one test must not satisfy the model's completion count (#2543)."""
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        # model.pkg.orders has 2 tests; the same test reported twice must not trigger aggregation.
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_not_called()
        # Only when the genuinely second test reports does the aggregated XCom fire.
        push_test_result_or_aggregate("test.pkg.unique_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_called_once()
        value = mock_xcom_push.call_args[1]["value"]
        assert value["status"] == DbtTestStatus.PASS
        assert value["total_count"] == 2


class TestPushTestResultOrAggregateConcurrency:
    """Tests that push_test_result_or_aggregate is thread-safe."""

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_concurrent_threads_do_not_lose_results(self, mock_xcom_push: MagicMock):
        """Simulate many concurrent dbt threads pushing test results for the same model.

        Without the lock, setdefault + item-assignment interleaving can lose results and
        the aggregated XCom may never fire or fire prematurely.
        """
        num_tests = 50
        test_ids = [f"test.pkg.test_{i}" for i in range(num_tests)]
        tests_per_model: dict[str, list[str]] = {"model.pkg.big_model": test_ids}
        results: ResultsTestsPerModel = {}
        ti = MagicMock()
        barrier = threading.Barrier(num_tests)

        def worker(test_id: str) -> None:
            barrier.wait()  # Force all threads to start at the same instant
            push_test_result_or_aggregate(test_id, "pass", tests_per_model, results, ti)

        threads = [threading.Thread(target=worker, args=(tid,)) for tid in test_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 50 results must be recorded — none lost to races
        assert len(results["model.pkg.big_model"]) == num_tests
        # XCom should have been pushed exactly once with a dict value
        mock_xcom_push.assert_called_once()
        value = mock_xcom_push.call_args[1]["value"]
        assert value["status"] == DbtTestStatus.PASS
        assert value["total_count"] == num_tests
