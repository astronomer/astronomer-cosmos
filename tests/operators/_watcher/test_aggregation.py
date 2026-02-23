"""Unit tests for cosmos.operators._watcher.aggregation module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cosmos.operators._watcher.aggregation import (
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


class TestAccumulateTestResult:
    """Tests for accumulate_test_result."""

    def test_returns_model_uid_when_test_found(self):
        results: dict[str, list[str]] = {}
        model_uid = accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        assert model_uid == "model.pkg.orders"
        assert results == {"model.pkg.orders": ["pass"]}

    def test_returns_none_when_test_not_found(self):
        results: dict[str, list[str]] = {}
        model_uid = accumulate_test_result("test.pkg.unknown_test", "pass", TESTS_PER_MODEL, results)
        assert model_uid is None
        assert results == {}

    def test_accumulates_multiple_results(self):
        results: dict[str, list[str]] = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.unique_orders_id", "fail", TESTS_PER_MODEL, results)
        assert results == {"model.pkg.orders": ["pass", "fail"]}

    def test_accumulates_across_models(self):
        results: dict[str, list[str]] = {}
        accumulate_test_result("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results)
        accumulate_test_result("test.pkg.not_null_customers_id", "pass", TESTS_PER_MODEL, results)
        assert results == {"model.pkg.orders": ["pass"], "model.pkg.customers": ["pass"]}


class TestGetAggregatedTestStatus:
    """Tests for get_aggregated_test_status."""

    def test_returns_none_when_model_not_in_tests_per_model(self):
        assert get_aggregated_test_status("model.pkg.unknown", TESTS_PER_MODEL, {}) is None

    def test_returns_none_when_not_all_tests_reported(self):
        results = {"model.pkg.orders": ["pass"]}
        assert get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results) is None

    def test_returns_pass_when_all_tests_pass(self):
        results = {"model.pkg.orders": ["pass", "pass"]}
        assert get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results) == DbtTestStatus.PASS

    def test_returns_fail_when_any_test_fails(self):
        results = {"model.pkg.orders": ["pass", "fail"]}
        assert get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results) == DbtTestStatus.FAIL

    def test_returns_fail_when_all_tests_fail(self):
        results = {"model.pkg.orders": ["fail", "fail"]}
        assert get_aggregated_test_status("model.pkg.orders", TESTS_PER_MODEL, results) == DbtTestStatus.FAIL

    def test_single_test_pass(self):
        results = {"model.pkg.customers": ["pass"]}
        assert get_aggregated_test_status("model.pkg.customers", TESTS_PER_MODEL, results) == DbtTestStatus.PASS

    def test_single_test_fail(self):
        results = {"model.pkg.customers": ["fail"]}
        assert get_aggregated_test_status("model.pkg.customers", TESTS_PER_MODEL, results) == DbtTestStatus.FAIL


class TestPushTestResultOrAggregate:
    """Tests for push_test_result_or_aggregate."""

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_pushes_xcom_when_all_tests_reported(self, mock_xcom_push: MagicMock):
        results: dict[str, list[str]] = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.unique_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_called_once_with(
            task_instance=ti,
            key="model__pkg__orders_tests_status",
            value=DbtTestStatus.PASS,
        )

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_does_not_push_xcom_before_all_tests_reported(self, mock_xcom_push: MagicMock):
        results: dict[str, list[str]] = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_not_called()

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_does_not_push_xcom_for_unknown_test(self, mock_xcom_push: MagicMock):
        results: dict[str, list[str]] = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.unknown", "pass", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_not_called()

    @patch("cosmos.operators._watcher.aggregation.safe_xcom_push")
    def test_pushes_fail_when_any_test_fails(self, mock_xcom_push: MagicMock):
        results: dict[str, list[str]] = {}
        ti = MagicMock()
        push_test_result_or_aggregate("test.pkg.not_null_orders_id", "pass", TESTS_PER_MODEL, results, ti)
        push_test_result_or_aggregate("test.pkg.unique_orders_id", "fail", TESTS_PER_MODEL, results, ti)
        mock_xcom_push.assert_called_once_with(
            task_instance=ti,
            key="model__pkg__orders_tests_status",
            value=DbtTestStatus.FAIL,
        )
