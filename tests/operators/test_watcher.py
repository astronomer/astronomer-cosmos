from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.state import DagRunState

try:
    from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_QUEUE
except ImportError:  # pragma: no cover
    from airflow.models.abstractoperator import DEFAULT_QUEUE  # type: ignore[no-redef]

from packaging.version import Version

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, TestBehavior
from cosmos.config import InvocationMode
from cosmos.constants import (
    _DBT_STARTUP_EVENTS_XCOM_KEY,
    PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT,
    DbtResourceType,
    ExecutionMode,
)
from cosmos.dbt.graph import DbtNode
from cosmos.operators._watcher.base import store_compiled_sql_for_model
from cosmos.operators._watcher.triggerer import WatcherEventReason, WatcherTrigger
from cosmos.operators.watcher import (
    DbtBuildWatcherOperator,
    DbtConsumerWatcherSensor,
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtTestWatcherOperator,
    _default_freshness_callback,
    store_dbt_resource_status_from_log,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping, get_automatic_profile_mapping
from tests.utils import AIRFLOW_VERSION, new_test_dag

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"
MULTI_FOLDER_DBT_PROJ_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt/multi_folder"

DBT_EXECUTABLE_PATH = Path(__file__).parent.parent.parent / "venv-subprocess/bin/dbt"
DBT_PROJECT_WITH_EMPTY_MODEL_PATH = Path(__file__).parent.parent / "sample/dbt_project_with_empty_model"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


class _MockTI:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.try_number = 1

    def xcom_push(self, key: str, value: str, **_):
        self.store[key] = value


class _MockContext(dict):
    pass


def test_dbt_producer_watcher_operator_priority_weight_default():
    """Test that DbtProducerWatcherOperator uses default priority_weight of 9999."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.priority_weight == PRODUCER_WATCHER_DEFAULT_PRIORITY_WEIGHT


@pytest.mark.parametrize(
    "queue_override, expected_queue",
    [
        ("custom_retry_queue", "custom_retry_queue"),
        (None, DEFAULT_QUEUE),
    ],
)
def test_dbt_producer_watcher_operator_queue(queue_override, expected_queue):
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", queue_override):
        op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)

        assert op.queue == expected_queue


@pytest.mark.integration
def test_producer_queue_from_setup_operator_args_when_both_set():
    """
    When both setup_operator_args queue and watcher_dbt_execution_queue are set,
    producer should use queue from watcher_dbt_execution_queue.
    """
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", "watcher_queue"):
        watcher_dag = DbtDag(
            project_config=project_config,
            profile_config=profile_config,
            start_date=datetime(2023, 1, 1),
            dag_id="watcher_dag_setup_queue",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
                setup_operator_args={"queue": "dbt_producer_task_queue"},
            ),
            render_config=RenderConfig(emit_datasets=False),
        )
    producer = watcher_dag.task_dict["dbt_producer_watcher"]
    assert producer.queue == "watcher_queue"


@pytest.mark.integration
def test_producer_queue_from_setup_operator_args():
    """
    When only setup_operator_args is set (no queue in watcher_dbt_execution_queue),
    producer should use queue from setup_operator_args.
    """
    with patch("cosmos.operators.watcher.watcher_dbt_execution_queue", None):
        watcher_dag = DbtDag(
            project_config=project_config,
            profile_config=profile_config,
            start_date=datetime(2023, 1, 1),
            dag_id="watcher_dag_watcher_queue_only",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
                setup_operator_args={"queue": "dbt_producer_task_queue"},
            ),
            render_config=RenderConfig(emit_datasets=False),
        )
    producer = watcher_dag.task_dict["dbt_producer_watcher"]
    assert producer.queue == "dbt_producer_task_queue"


def test_dbt_producer_watcher_operator_priority_weight_override():
    """Test that DbtProducerWatcherOperator allows overriding priority_weight."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, priority_weight=100)
    assert op.priority_weight == 100


def test_dbt_producer_log_format_always_json():
    """WATCHER always uses --log-format json regardless of any invocation_mode hint passed."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.log_format == "json"


def test_dbt_producer_watcher_operator_pushes_completion_status():
    """Test that operator pushes 'completed' status to XCom in both success and failure cases."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    mock_ti = _MockTI()
    context = {"ti": mock_ti}

    # Test success case
    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        op.execute(context=context)

        # Verify status was pushed
        assert mock_ti.store.get("task_status") == "completed"
        # Verify parent execute was called
        mock_execute.assert_called_once()

    # Reset mock and store
    mock_ti.store.clear()

    # Test failure case
    class TestException(Exception):
        pass

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        mock_execute.side_effect = TestException("test error")

        with pytest.raises(TestException):
            op.execute(context=context)

        # Verify completed status was pushed even in failure case
        assert mock_ti.store.get("task_status") == "completed"
        # Verify parent execute was called
        mock_execute.assert_called_once()


def test_dbt_producer_watcher_operator_requires_task_instance():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    context: dict[str, object] = {}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        with pytest.raises(AirflowException) as excinfo:
            op.execute(context=context)

    mock_execute.assert_not_called()
    assert "expects a task instance" in str(excinfo.value)


def test_dbt_consumer_watcher_sensor_execute_complete_model_not_run_logs_message(caplog):
    """Test that execute_complete logs an info message when model was skipped (node_not_run)."""
    sensor = DbtConsumerWatcherSensor(
        project_dir=".",
        profiles_dir=".",
        profile_config=profile_config,
        model_unique_id="model.pkg.skipped_model",
        poke_interval=1,
        producer_task_id="dbt_producer_watcher_operator",
        task_id="consumer_sensor",
    )
    sensor.model_unique_id = "model.pkg.skipped_model"

    context = {"dag_run": MagicMock()}
    event = {"status": "success", "reason": WatcherEventReason.NODE_NOT_RUN}

    with caplog.at_level(logging.INFO):
        sensor.execute_complete(context, event)

    assert any(
        "Model 'model.pkg.skipped_model' was skipped by the dbt command" in message for message in caplog.messages
    )
    assert any("ephemeral model or if the model sql file is empty" in message for message in caplog.messages)


def test_dbt_producer_watcher_operator_skips_retry_attempt(caplog):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ti.try_number = 2
    context = {"ti": ti}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.execute") as mock_execute:
        with caplog.at_level(logging.INFO):
            result = op.execute(context=context)

    mock_execute.assert_not_called()
    assert result is None
    assert any("does not support Airflow retries" in message for message in caplog.messages)
    assert any("skipping execution" in message for message in caplog.messages)


@pytest.mark.parametrize(
    "event, expected_message",
    [
        ({"status": "success"}, None),
        ({"status": "success", "reason": WatcherEventReason.NODE_NOT_RUN}, None),
        (
            {"status": "failed", "reason": WatcherEventReason.NODE_FAILED},
            "dbt model 'model.pkg.m' failed. Review the producer task 'dbt_producer_watcher_operator' logs for details.",
        ),
        (
            {"status": "failed", "reason": WatcherEventReason.PRODUCER_FAILED},
            "Watcher producer task 'dbt_producer_watcher_operator' failed before reporting results for model 'model.pkg.m'. Check its logs for the underlying error.",
        ),
    ],
)
def test_dbt_consumer_watcher_sensor_execute_complete(event, expected_message):
    sensor = DbtConsumerWatcherSensor(
        project_dir=".",
        profiles_dir=".",
        profile_config=profile_config,
        model_unique_id="model.pkg.m",
        poke_interval=1,
        producer_task_id="dbt_producer_watcher_operator",
        task_id="consumer_sensor",
    )
    sensor.model_unique_id = "model.pkg.m"

    ti = MagicMock()
    ti.xcom_pull.return_value = None
    context = {"dag_run": MagicMock(), "ti": ti}

    if expected_message is None:
        sensor.execute_complete(context, event)
        return

    with pytest.raises(AirflowException) as excinfo:
        sensor.execute_complete(context, event)

    assert str(excinfo.value) == expected_message


class TestStoreDbtStatusFromLog:
    """Tests for store_dbt_resource_status_from_log and _process_log_line_callable."""

    def test_store_dbt_resource_status_from_log_success(self):
        """Test that success status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"node_info": {"node_status": "success", "unique_id": "model.pkg.my_model"}}})

        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        assert ti.store.get("model__pkg__my_model_status") == "success"

    def test_store_dbt_resource_status_from_log_failed(self):
        """Test that failed status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"node_info": {"node_status": "failed", "unique_id": "model.pkg.failed_model"}}})

        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        assert ti.store.get("model__pkg__failed_model_status") == "failed"

    def test_store_dbt_resource_status_from_log_ignores_other_statuses(self):
        """Test that statuses other than success/failed are ignored."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps(
            {"data": {"node_info": {"node_status": "running", "unique_id": "model.pkg.running_model"}}}
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        assert "model__pkg__running_model_status" not in ti.store

    def test_store_dbt_resources_status_from_log_detects_passed_test_status(self):
        """Test that a passed test status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "pass",
                        "unique_id": "test.pkg.my_test",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        assert ti.store.get("test__pkg__my_test_status") == "pass"

    def test_store_dbt_resource_status_from_log_detects_failed_test_status(self):
        """Test that a failed test status is correctly parsed and stored in XCom."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "fail",
                        "unique_id": "test.pkg.my_test",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        assert ti.store.get("test__pkg__my_test_status") == "fail"

    def test_store_dbt_resource_status_from_log_aggregates_test_results_when_tests_per_model_provided(self):
        """When tests_per_model is non-empty and a test node finishes, the function should
        accumulate results and push a single aggregated *_tests_status XCom once all tests
        for the model have reported — instead of pushing individual *_status keys per test.
        """
        ti = _MockTI()
        ctx = {"ti": ti}

        tests_per_model = {
            "model.pkg.orders": ["test.pkg.not_null_orders_id", "test.pkg.unique_orders_id"],
        }
        test_results_per_model: dict[str, list[str]] = {}

        # First test passes — not all tests reported yet, no XCom push
        log_line_1 = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "pass",
                        "unique_id": "test.pkg.not_null_orders_id",
                        "resource_type": "test",
                    }
                }
            }
        )
        store_dbt_resource_status_from_log(
            log_line_1,
            {"context": ctx},
            tests_per_model=tests_per_model,
            test_results_per_model=test_results_per_model,
        )
        assert "test__pkg__not_null_orders_id_status" not in ti.store  # no per-test key
        assert "model__pkg__orders_tests_status" not in ti.store  # not yet aggregated

        # Second test passes — all tests reported, aggregated XCom should be pushed
        log_line_2 = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "pass",
                        "unique_id": "test.pkg.unique_orders_id",
                        "resource_type": "test",
                    }
                }
            }
        )
        store_dbt_resource_status_from_log(
            log_line_2,
            {"context": ctx},
            tests_per_model=tests_per_model,
            test_results_per_model=test_results_per_model,
        )
        assert "test__pkg__unique_orders_id_status" not in ti.store  # no per-test key
        assert ti.store.get("model__pkg__orders_tests_status") == "pass"

    def test_store_dbt_resource_status_from_log_aggregates_fail_when_any_test_fails(self):
        """When at least one test fails, the aggregated status should be 'fail'."""
        ti = _MockTI()
        ctx = {"ti": ti}

        tests_per_model = {
            "model.pkg.orders": ["test.pkg.not_null_orders_id", "test.pkg.unique_orders_id"],
        }
        test_results_per_model: dict[str, list[str]] = {}

        for uid, status in [
            ("test.pkg.not_null_orders_id", "pass"),
            ("test.pkg.unique_orders_id", "fail"),
        ]:
            log_line = json.dumps(
                {"data": {"node_info": {"node_status": status, "unique_id": uid, "resource_type": "test"}}}
            )
            store_dbt_resource_status_from_log(
                log_line,
                {"context": ctx},
                tests_per_model=tests_per_model,
                test_results_per_model=test_results_per_model,
            )

        assert ti.store.get("model__pkg__orders_tests_status") == "fail"
        # No per-test status keys should exist
        assert "test__pkg__not_null_orders_id_status" not in ti.store
        assert "test__pkg__unique_orders_id_status" not in ti.store
        """Test that invalid JSON doesn't raise an exception."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Should not raise an exception
        store_dbt_resource_status_from_log(
            "not valid json {{{", {"context": ctx}, tests_per_model={}, test_results_per_model={}
        )

        # No status should be stored
        assert len(ti.store) == 0

    def test_store_dbt_resource_status_from_log_handles_missing_node_info(self):
        """Test that missing node_info doesn't raise an exception."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"data": {"other_key": "value"}})

        # Should not raise an exception
        store_dbt_resource_status_from_log(log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={})

        # No status should be stored
        assert len(ti.store) == 0

    @pytest.mark.parametrize(
        "msg, level",
        [
            ("Running with dbt=1.10.11", "info"),
            ("This is a warning", "warning"),
            ("An error occurred", "error"),
            ("Debugging info", "debug"),
            ("Unknown level defaults to INFO", "unknown"),  # just to ensure it defaults
        ],
    )
    def test_store_dbt_resource_status_from_log_outputs_dbt_info(self, caplog, msg, level):
        """Test that dbt info messages are logged correctly."""
        ti = _MockTI()
        ctx = {"ti": ti}

        log_line = json.dumps({"info": {"msg": msg, "level": level}})
        dynamic_level = getattr(logging, level.upper(), logging.INFO)
        with caplog.at_level(dynamic_level):
            store_dbt_resource_status_from_log(
                log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={}
            )

        assert msg in caplog.text
        assert any(record.levelname == logging.getLevelName(dynamic_level) for record in caplog.records)

    def test_store_dbt_resource_status_from_log_logs_message_only_once(self, caplog):
        """Test that dbt log messages are logged exactly once (no duplicates)."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "1 of 5 START sql view model release_17.stg_customers"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": "2025-01-29T13:16:05.123456Z"}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(
                log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={}
            )

        # Count how many times the message appears in log records
        message_count = sum(1 for record in caplog.records if test_msg in record.message)
        assert message_count == 1, f"Expected message to be logged exactly once, but found {message_count} times"

    def test_store_dbt_resource_status_from_log_formats_timestamp(self, caplog):
        """Test that the timestamp is formatted as HH:MM:SS to match dbt runner format."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "Running with dbt=1.10.11"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": "2025-01-29T13:16:05.123456Z"}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(
                log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={}
            )

        # Verify the timestamp is formatted as HH:MM:SS
        assert any("13:16:05" in record.message and test_msg in record.message for record in caplog.records)

    def test_store_dbt_resource_status_from_log_invalid_timestamp_falls_back_to_raw(self, caplog):
        """Test that invalid timestamps fall back to raw value instead of raising an error."""
        ti = _MockTI()
        ctx = {"ti": ti}

        test_msg = "Running with dbt=1.10.11"
        # Looks like a valid ISO timestamp but has invalid month (13) - triggers ValueError in fromisoformat()
        invalid_ts = "2025-13-29T13:16:05.123456Z"
        log_line = json.dumps({"info": {"msg": test_msg, "level": "info", "ts": invalid_ts}})

        with caplog.at_level(logging.INFO):
            store_dbt_resource_status_from_log(
                log_line, {"context": ctx}, tests_per_model={}, test_results_per_model={}
            )

        # Verify the raw timestamp is used when parsing fails
        assert any(invalid_ts in record.message and test_msg in record.message for record in caplog.records)

    def test_process_log_line_callable_integration_with_subprocess_pattern(self):
        """Test the exact pattern used in subprocess.py: process_log_line(line, kwargs).

        The production code uses functools.partial to bind tests_per_model,
        so the subprocess hook can still call process_log_line(line, kwargs) with 2 positional args.
        """
        import functools

        op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
        op._process_log_line_callable = functools.partial(
            store_dbt_resource_status_from_log, tests_per_model={}, test_results_per_model={}
        )

        ti = _MockTI()
        ctx = {"ti": ti}

        # Simulate the kwargs dict that subprocess.py passes
        kwargs = {"context": ctx, "other_param": "value"}

        log_lines = [
            json.dumps({"data": {"node_info": {"node_status": "success", "unique_id": "model.pkg.model_a"}}}),
            json.dumps({"data": {"node_info": {"node_status": "failed", "unique_id": "model.pkg.model_b"}}}),
            json.dumps({"info": {"msg": "Running with dbt=1.10.11"}}),  # Non-node log line
        ]

        # Simulate the subprocess.py pattern
        for line in log_lines:
            op._process_log_line_callable(line, kwargs)

        assert ti.store.get("model__pkg__model_a_status") == "success"
        assert ti.store.get("model__pkg__model_b_status") == "failed"
        assert len(ti.store) == 2  # Only success and failed statuses are stored

    def test_store_dbt_resource_status_from_log_pushes_compiled_sql_for_models(self, tmp_path):
        """Test that compiled_sql is pushed to XCom for successful model nodes."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Create a fake compiled SQL file
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        compiled_sql_file = compiled_dir / "my_model.sql"
        compiled_sql_file.write_text("SELECT * FROM orders WHERE status = 'completed'")

        # JSON log with node_path and resource_type
        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "success",
                        "unique_id": "model.pkg.my_model",
                        "node_path": "my_model.sql",
                        "resource_type": "model",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("model__pkg__my_model_status") == "success"
        assert ti.store.get("model__pkg__my_model_compiled_sql") == "SELECT * FROM orders WHERE status = 'completed'"

    def test_store_dbt_resource_status_from_log_no_compiled_sql_for_non_models(self, tmp_path):
        """Test that compiled_sql is not pushed for non-model resources like seeds or tests."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # JSON log for a seed (not a model)
        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "success",
                        "unique_id": "seed.pkg.my_seed",
                        "node_path": "my_seed.csv",
                        "resource_type": "seed",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("seed__pkg__my_seed_status") == "success"
        assert "seed__pkg__my_seed_compiled_sql" not in ti.store

    def test_store_dbt_resource_status_from_log_pushes_compiled_sql_on_failure(self, tmp_path):
        """Test that compiled_sql is pushed for failed models too (useful for debugging)."""
        ti = _MockTI()
        ctx = {"ti": ti}

        # Create a fake compiled SQL file (compilation happens before execution, so it exists even if model fails)
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        compiled_sql_file = compiled_dir / "failed_model.sql"
        compiled_sql_file.write_text("SELECT * FROM orders WHERE invalid_column = 1")

        log_line = json.dumps(
            {
                "data": {
                    "node_info": {
                        "node_status": "failed",
                        "unique_id": "model.pkg.failed_model",
                        "node_path": "failed_model.sql",
                        "resource_type": "model",
                    }
                }
            }
        )

        store_dbt_resource_status_from_log(log_line, {"context": ctx, "project_dir": str(tmp_path)})

        assert ti.store.get("model__pkg__failed_model_status") == "failed"
        assert ti.store.get("model__pkg__failed_model_compiled_sql") == "SELECT * FROM orders WHERE invalid_column = 1"


class TestStoreCompiledSqlForModelPathHandling:
    """Tests for store_compiled_sql_for_model (node_path is relative to target/compiled/<package>/models/)."""

    def test_missing_node_path_does_not_push(self, tmp_path):
        """When node_path is None or empty, no compiled_sql is extracted or pushed."""
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.m", None, "model")
        assert "model__pkg__m_compiled_sql" not in ti.store

        ti2 = _MockTI()
        store_compiled_sql_for_model(ti2, str(tmp_path), "model.pkg.m", "", "model")
        assert "model__pkg__m_compiled_sql" not in ti2.store

    def test_nonexistent_path_does_not_push(self, tmp_path):
        """When compiled SQL path does not exist, we do not push."""
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.my_model", "nonexistent.sql", "model")
        assert "model__pkg__my_model_compiled_sql" not in ti.store

    def test_path_traversal_outside_project_does_not_push(self, tmp_path):
        """node_path with .. that resolves outside compiled root: file does not exist, so we do not push."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "legit.sql").write_text("SELECT 1")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.m", "../../../etc/passwd", "model")
        assert "model__pkg__m_compiled_sql" not in ti.store

    def test_compiled_sql_under_models_pushed(self, tmp_path):
        """node_path relative to models/ (e.g. staging/foo.sql) is read and pushed."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models" / "staging"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "stg_orders.sql").write_text("SELECT * FROM staging.orders")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.stg_orders", "staging/stg_orders.sql", "model")
        assert ti.store.get("model__pkg__stg_orders_compiled_sql") == "SELECT * FROM staging.orders"

    def test_compiled_sql_flat_path_pushed(self, tmp_path):
        """node_path as single file under models/ (e.g. foo.sql) is read and pushed."""
        compiled_dir = tmp_path / "target" / "compiled" / "pkg" / "models"
        compiled_dir.mkdir(parents=True)
        (compiled_dir / "foo.sql").write_text("SELECT 1")
        ti = _MockTI()
        store_compiled_sql_for_model(ti, str(tmp_path), "model.pkg.foo", "foo.sql", "model")
        assert ti.store.get("model__pkg__foo_compiled_sql") == "SELECT 1"


def test_producer_does_not_force_invocation_mode():
    """DbtProducerWatcherOperator does not force an invocation_mode; auto-discovery runs at runtime."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.invocation_mode is None  # resolved lazily by _discover_invocation_mode()


def test_producer_respects_explicit_invocation_mode():
    """An explicit invocation_mode passed by the caller is preserved unchanged."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, invocation_mode=InvocationMode.SUBPROCESS)
    assert op.invocation_mode == InvocationMode.SUBPROCESS

    op2 = DbtProducerWatcherOperator(project_dir=".", profile_config=None, invocation_mode=InvocationMode.DBT_RUNNER)
    assert op2.invocation_mode == InvocationMode.DBT_RUNNER


def test_run_subprocess_sets_process_log_line_callable():
    """run_subprocess wires up _process_log_line_callable before executing the subprocess."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op._process_log_line_callable is None

    with patch("cosmos.operators.local.DbtLocalBaseOperator.run_subprocess", return_value=MagicMock()):
        op.run_subprocess(command=["dbt", "build"], env={}, cwd="/tmp/proj")

    assert op._process_log_line_callable is not None


def test_run_dbt_runner_registers_event_callback():
    """run_dbt_runner appends an EventMsg→JSON→parse callback to _dbt_runner_callbacks."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert not op._dbt_runner_callbacks

    mock_ti = _MockTI()
    context = {"ti": mock_ti, "run_id": "run-1"}

    with patch("cosmos.operators.local.DbtLocalBaseOperator.run_dbt_runner", return_value=MagicMock()):
        op.run_dbt_runner(command=["dbt", "build"], env={}, cwd="/tmp/proj", context=context)

    assert len(op._dbt_runner_callbacks) == 1


def test_run_dbt_runner_event_callback_calls_store_from_log():
    """The registered callback converts an EventMsg to JSON and passes it to store_dbt_resource_status_from_log."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    mock_ti = _MockTI()
    context = {"ti": mock_ti, "run_id": "run-1"}

    fake_json = '{"info": {"name": "NodeFinished"}, "data": {}}'
    fake_event = MagicMock()

    # Patch store_dbt_resource_status_from_log *before* run_dbt_runner so that _make_parse_callable
    # captures the mock through functools.partial, not the real function.
    with (
        patch("cosmos.operators.local.DbtLocalBaseOperator.run_dbt_runner", return_value=MagicMock()),
        patch("cosmos.operators.watcher.store_dbt_resource_status_from_log") as mock_parse,
        patch("google.protobuf.json_format.MessageToJson", return_value=fake_json) as mock_to_json,
    ):
        op.run_dbt_runner(command=["dbt", "build"], env={}, cwd="/tmp/proj", context=context)
        callback = op._dbt_runner_callbacks[0]
        callback(fake_event)

    mock_to_json.assert_called_once_with(fake_event, preserving_proto_field_name=True)
    mock_parse.assert_called_once()
    call_args = mock_parse.call_args
    assert call_args[0][0] == fake_json  # first positional arg is the JSON string
    assert call_args[0][1]["project_dir"] == "/tmp/proj"
    assert call_args[0][1]["context"] is context


def test_run_dbt_runner_callback_error_fails_producer_after_run(caplog):
    """A callback error must not surface as GenericExceptionOnRun inside dbt; instead it must be
    re-raised after the dbt run so it propagates through execute() and triggers the task_status
    XCom push that signals consumer sensors to check the producer task state."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    context = {"ti": _MockTI(), "run_id": "run-1"}

    def fake_run_dbt_runner(self_inner, command, env, cwd, **kw):
        # Simulate dbt calling the registered callback for one event, as the real runner would.
        for cb in op._dbt_runner_callbacks or []:
            cb(MagicMock())

    with (
        patch("cosmos.operators.local.DbtLocalBaseOperator.run_dbt_runner", fake_run_dbt_runner),
        patch("google.protobuf.json_format.MessageToJson", side_effect=RuntimeError("serialisation error")),
        caplog.at_level(logging.ERROR),
        pytest.raises(RuntimeError, match="serialisation error"),
    ):
        op.run_dbt_runner(command=["dbt", "build"], env={}, cwd="/tmp/proj", context=context)

    assert "Error in dbt event callback" in caplog.text


MODEL_UNIQUE_ID = "model.jaffle_shop.stg_orders"


class TestDbtConsumerWatcherSensor:
    def make_sensor(self, **kwargs):
        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        kwargs["extra_context"] = extra_context
        sensor = DbtConsumerWatcherSensor(
            task_id="model.my_model",
            project_dir="/tmp/project",
            profile_config=None,
            deferrable=kwargs.pop("deferrable", True),
            **kwargs,
        )

        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    def make_context(self, ti_mock, *, run_id: str = "test-run", map_index: int = 0):
        return {
            "ti": ti_mock,
            "run_id": run_id,
            "task_instance": MagicMock(map_index=map_index),
        }

    @pytest.mark.skipif(AIRFLOW_VERSION >= Version("3.0.0"), reason="RuntimeTaskInstance path in Airflow >= 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("2.9.0"))
    def test_get_producer_task_status_airflow2(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_1")

        fetcher = MagicMock(return_value="success")

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=fetcher) as mock_builder:
            status = sensor._get_producer_task_status(context)

        mock_builder.assert_called_once_with(
            airflow_version=Version("2.9.0"),
            dag_id="example_dag",
            run_id="run_1",
            producer_task_id=sensor.producer_task_id,
            logger=ANY,
        )
        fetcher.assert_called_once_with()
        assert status == "success"

    @pytest.mark.skipif(AIRFLOW_VERSION >= Version("3.0.0"), reason="RuntimeTaskInstance path in Airflow >= 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("2.9.0"))
    def test_get_producer_task_status_airflow2_missing_instance(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_2")

        fetcher = MagicMock(return_value=None)

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=fetcher):
            status = sensor._get_producer_task_status(context)

        fetcher.assert_called_once_with()
        assert status is None

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    @patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states")
    def test_get_producer_task_status_airflow3(self, mock_get_task_states):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_3")

        mock_get_task_states.return_value = {"run_3": {sensor.producer_task_id: "running"}}

        status = sensor._get_producer_task_status(context)

        assert status == "running"
        mock_get_task_states.assert_called_once_with(
            dag_id="example_dag", task_ids=[sensor.producer_task_id], run_ids=["run_3"]
        )

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    @patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states")
    def test_get_producer_task_status_airflow3_missing_state(self, mock_get_task_states):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_3_missing")

        mock_get_task_states.return_value = {"run_3_missing": {}}

        status = sensor._get_producer_task_status(context)

        assert status is None
        mock_get_task_states.assert_called_once_with(
            dag_id="example_dag", task_ids=[sensor.producer_task_id], run_ids=["run_3_missing"]
        )

    @pytest.mark.skipif(AIRFLOW_VERSION < Version("3.0.0"), reason="Database lookup path in Airflow < 3.0")
    @patch("cosmos.operators._watcher.base.AIRFLOW_VERSION", new=Version("3.0.0"))
    def test_get_producer_task_status_airflow3_import_error(self):
        sensor = self.make_sensor()
        sensor._get_producer_task_status = DbtConsumerWatcherSensor._get_producer_task_status.__get__(
            sensor, DbtConsumerWatcherSensor
        )
        ti = MagicMock()
        ti.dag_id = "example_dag"
        context = self.make_context(ti, run_id="run_4")

        with patch("cosmos.operators._watcher.base.build_producer_state_fetcher", return_value=None) as mock_builder:
            status = sensor._get_producer_task_status(context)

        mock_builder.assert_called_once_with(
            airflow_version=Version("3.0.0"),
            dag_id="example_dag",
            run_id="run_4",
            producer_task_id=sensor.producer_task_id,
            logger=ANY,
        )
        assert status is None

    @patch("cosmos.operators._watcher.base.BaseConsumerSensor._log_startup_events")
    def test_poke_status_none(self, mock_startup_events):
        """poke returns False when no status has been written to XCom yet."""
        sensor = self.make_sensor()

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is False

    def test_poke_success(self):
        sensor = self.make_sensor()

        ti = MagicMock()
        ti.try_number = 1
        # xcom_pull calls: _log_startup_events=None, _get_node_status="success", compiled_sql=None, _dbt_event=None
        ti.xcom_pull.side_effect = [None, "success", None, None]
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_poke_subprocess_mode_extracts_compiled_sql_from_xcom(self, mock_override_rtif):
        """Test that in subprocess mode, poke extracts compiled_sql from per-model XCom key when status is success."""
        sensor = self.make_sensor()
        sensor.invocation_mode = "SUBPROCESS"
        sensor.model_unique_id = MODEL_UNIQUE_ID

        ti = MagicMock()
        ti.try_number = 1
        # xcom_pull calls: _log_startup_events=None, _get_node_status="success", compiled_sql="SELECT * FROM orders", _dbt_event=None
        ti.xcom_pull.side_effect = [None, "success", "SELECT * FROM orders", None]
        context = self.make_context(ti)

        assert sensor.compiled_sql == ""  # Initially empty
        result = sensor.poke(context)
        assert result is True
        assert sensor.compiled_sql == "SELECT * FROM orders"
        mock_override_rtif.assert_called_once_with(context)

    def test_poke_failure(self):
        """poke raises AirflowException when model status is a failure value."""
        sensor = self.make_sensor()

        ti = MagicMock()
        ti.try_number = 1
        # xcom_pull calls: _log_startup_events=None, _get_node_status="failed", compiled_sql=None, _dbt_event=None
        ti.xcom_pull.side_effect = [None, "failed", None, None]
        context = self.make_context(ti)

        with pytest.raises(AirflowException):
            sensor.poke(context)

    @patch("cosmos.operators.local.AbstractDbtLocalBase.build_and_run_cmd")
    def test_task_retry(self, mock_build_and_run_cmd):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 2
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        sensor.poke(context)
        mock_build_and_run_cmd.assert_called_once()

    def test_fallback_to_non_watcher_run(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--select", "some_model", "--threads", "2"]
        context = self.make_context(ti)
        sensor.build_and_run_cmd = MagicMock()

        result = sensor._fallback_to_non_watcher_run(2, context)

        assert result is True
        sensor.build_and_run_cmd.assert_called_once()
        args, kwargs = sensor.build_and_run_cmd.call_args
        assert "--select" in kwargs["cmd_flags"]
        assert MODEL_UNIQUE_ID.split(".")[-1] in kwargs["cmd_flags"]

    def test_filter_flags(self):
        flags = ["--select", "model", "--exclude", "other", "--threads", "2"]
        expected = ["--threads", "2"]

        result = DbtConsumerWatcherSensor._filter_flags(flags)

        assert result == expected

    @patch("cosmos.operators._watcher.base.get_xcom_val")
    def test_producer_state_failed(self, mock_get_xcom_val):
        sensor = self.make_sensor()
        sensor._get_producer_task_status.return_value = "failed"
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 1
        mock_get_xcom_val.return_value = None
        # _log_startup_events still calls ti.xcom_pull directly
        ti.xcom_pull.return_value = None

        context = self.make_context(ti)

        with pytest.raises(
            AirflowException,
            match="The dbt build command failed in producer task. Please check the log of task dbt_producer_watcher for details.",
        ):
            sensor.poke(context)

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor._fallback_to_non_watcher_run")
    @patch("cosmos.operators._watcher.base.get_xcom_val")
    def test_producer_state_does_not_fail_if_previously_upstream_failed(
        self, mock_get_xcom_val, mock_fallback_to_non_watcher_run
    ):
        """
        Attempt to run the task using ExecutionMode.LOCAL if State.UPSTREAM_FAILED happens.
        More details: https://github.com/astronomer/astronomer-cosmos/pull/2062
        """
        sensor = self.make_sensor()
        sensor._get_producer_task_status.return_value = "failed"
        ti = MagicMock()
        ti.try_number = 1
        sensor.poke_retry_number = 0
        mock_get_xcom_val.return_value = None
        # _log_startup_events still calls ti.xcom_pull directly
        ti.xcom_pull.return_value = None

        context = self.make_context(ti)

        sensor.poke(context)
        mock_fallback_to_non_watcher_run.assert_called_once()

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_sensor_deferred(self, mock_poke):
        mock_poke.return_value = False
        sensor = self.make_sensor()
        context = {"run_id": "run_id", "task_instance": Mock()}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, WatcherTrigger), "Trigger is not a WatcherTrigger"

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_sensor_not_deferred(self, mock_poke):
        sensor = self.make_sensor()
        sensor.deferrable = False
        context = {"run_id": "run_id", "task_instance": Mock()}
        sensor.execute(context=context)
        mock_poke.assert_called_once()

    @patch("cosmos.operators.watcher.DbtConsumerWatcherSensor.poke")
    def test_deferrable_false_via_constructor_does_not_defer(self, mock_poke):
        """operator_args={'deferrable': False} is respected: sensor created with deferrable=False does not defer."""
        mock_poke.return_value = True
        sensor = self.make_sensor(deferrable=False)
        assert sensor.deferrable is False
        context = {"run_id": "run_id", "task_instance": Mock()}
        sensor.execute(context=context)
        mock_poke.assert_called_once()
        # No TaskDeferred raised: sensor ran synchronously and completed

    @pytest.mark.parametrize(
        "mock_event",
        [
            {"status": "failed", "reason": WatcherEventReason.NODE_FAILED},
            {"status": "failed", "reason": WatcherEventReason.PRODUCER_FAILED},
            {"status": "success"},
        ],
    )
    def test_execute_complete(self, mock_event):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)
        if mock_event.get("status") == "failed":
            with pytest.raises(AirflowException):
                sensor.execute_complete(context=context, event=mock_event)
        else:
            assert sensor.execute_complete(context=context, event=mock_event) is None

    @patch("cosmos.operators.local.AbstractDbtLocalBase._override_rtif")
    def test_execute_complete_extracts_compiled_sql(self, mock_override_rtif):
        """Test that execute_complete extracts compiled_sql from the event and sets it on the sensor."""
        sensor = self.make_sensor()
        context = Mock()

        assert sensor.compiled_sql == ""  # Initially empty

        event = {"status": "success", "compiled_sql": "SELECT * FROM orders WHERE status = 'active'"}
        sensor.execute_complete(context=context, event=event)

        assert sensor.compiled_sql == "SELECT * FROM orders WHERE status = 'active'"
        mock_override_rtif.assert_called_once_with(context)

    def test_execute_complete_handles_missing_compiled_sql(self):
        """Test that execute_complete handles events without compiled_sql gracefully."""
        sensor = self.make_sensor()
        context = Mock()

        assert sensor.compiled_sql == ""  # Initially empty

        event = {"status": "success"}  # No compiled_sql in event
        sensor.execute_complete(context=context, event=event)

        assert sensor.compiled_sql == ""  # Should remain empty

    @patch("cosmos.operators._watcher.base.settings")
    def test_execute_debug_mode_tracks_memory_on_success(self, mock_settings):
        """Memory tracking is started and stopped when poke() returns True (task completes without deferral)."""
        mock_settings.enable_debug_mode = True
        sensor = self.make_sensor()
        ti = MagicMock()
        context = self.make_context(ti)

        with (
            patch("cosmos.debug.start_memory_tracking") as mock_start,
            patch("cosmos.debug.stop_memory_tracking") as mock_stop,
            patch.object(sensor, "poke", return_value=True),
        ):
            sensor.execute(context=context)

        mock_start.assert_called_once_with(context)
        mock_stop.assert_called_once_with(context)

    @patch("cosmos.operators._watcher.base.settings")
    def test_execute_debug_mode_tracks_memory_on_defer(self, mock_settings):
        """Memory tracking is started and stopped even when the task defers (TaskDeferred is BaseException)."""
        mock_settings.enable_debug_mode = True
        sensor = self.make_sensor(deferrable=True)
        ti = MagicMock()
        context = self.make_context(ti)

        with (
            patch("cosmos.debug.start_memory_tracking") as mock_start,
            patch("cosmos.debug.stop_memory_tracking") as mock_stop,
            patch.object(sensor, "poke", return_value=False),
            pytest.raises(TaskDeferred),
        ):
            sensor.execute(context=context)

        mock_start.assert_called_once_with(context)
        mock_stop.assert_called_once_with(context)

    @patch("cosmos.operators._watcher.base.settings")
    def test_execute_debug_mode_tracks_memory_non_deferrable(self, mock_settings):
        """Memory tracking is started and stopped on the non-deferrable path (super().execute() loop)."""
        mock_settings.enable_debug_mode = True
        sensor = self.make_sensor(deferrable=False)
        ti = MagicMock()
        context = self.make_context(ti)

        with (
            patch("cosmos.debug.start_memory_tracking") as mock_start,
            patch("cosmos.debug.stop_memory_tracking") as mock_stop,
            patch("cosmos.operators._watcher.base.BaseSensorOperator.execute") as mock_super_execute,
        ):
            sensor.execute(context=context)

        mock_super_execute.assert_called_once_with(context)
        mock_start.assert_called_once_with(context)
        mock_stop.assert_called_once_with(context)


class TestDbtBuildWatcherOperator:
    def test_dbt_build_watcher_operator_raises_not_implemented_error(self):
        expected_message = (
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )

        with pytest.raises(NotImplementedError, match=expected_message):
            DbtBuildWatcherOperator()


class TestWatcherTrigger:
    """Tests for WatcherTrigger compiled_sql extraction."""

    def make_trigger(self):
        return WatcherTrigger(
            model_unique_id="model.pkg.my_model",
            producer_task_id="dbt_producer_watcher",
            dag_id="test_dag",
            run_id="test_run",
            map_index=None,
            poke_interval=1.0,
        )

    @pytest.mark.asyncio
    async def test_parse_dbt_node_status_and_compiled_sql_subprocess_mode(self):
        """Test that compiled_sql is extracted from XCom in subprocess mode."""
        trigger = self.make_trigger()

        # Mock get_xcom_val to return status and compiled_sql
        async def mock_get_xcom_val(key):
            if key == "model__pkg__my_model_status":
                return "success"
            elif key == "model__pkg__my_model_compiled_sql":
                return "SELECT * FROM orders"
            return None

        trigger.get_xcom_val = mock_get_xcom_val

        status, compiled_sql = await trigger._parse_dbt_node_status_and_compiled_sql()

        assert status == "success"
        assert compiled_sql == "SELECT * FROM orders"

    @pytest.mark.asyncio
    async def test_parse_dbt_node_status_and_compiled_sql_subprocess_no_compiled_sql(self):
        """Test that missing compiled_sql is handled gracefully in subprocess mode."""
        trigger = self.make_trigger()

        # Mock get_xcom_val to return only status
        async def mock_get_xcom_val(key):
            if key == "model__pkg__my_model_status":
                return "success"
            return None

        trigger.get_xcom_val = mock_get_xcom_val

        status, compiled_sql = await trigger._parse_dbt_node_status_and_compiled_sql()

        assert status == "success"
        assert compiled_sql is None

    @pytest.mark.asyncio
    async def test_log_startup_events_returns_when_events_available(self, caplog):
        """Test that _log_startup_events returns once dbt_startup_events is available and logs."""
        trigger = self.make_trigger()
        events = [
            {"name": "MainReportVersion", "msg": "Running with dbt=1.10.0", "ts": "2025-01-01T12:00:00Z"},
            {"name": "AdapterRegistered", "msg": "Registered adapter: postgres=1.10.0", "ts": "2025-01-01T12:00:01Z"},
        ]
        call_count = 0

        async def mock_get_xcom_val(key):
            nonlocal call_count
            call_count += 1
            if key == _DBT_STARTUP_EVENTS_XCOM_KEY:
                return events
            return None

        async def mock_producer_running():
            return None  # not failed

        trigger.get_xcom_val = mock_get_xcom_val
        trigger._get_producer_task_status = mock_producer_running

        with caplog.at_level(logging.INFO):
            await trigger._log_startup_events()

        assert "Running with dbt=1.10.0" in caplog.text
        assert "Registered adapter: postgres=1.10.0" in caplog.text
        assert call_count >= 1

    @pytest.mark.asyncio
    async def test_wait_and_log_startup_events_returns_when_producer_failed(self):
        """Test that _log_startup_events returns without blocking when producer task failed."""
        trigger = self.make_trigger()
        call_count = 0

        async def mock_get_xcom_val(key):
            nonlocal call_count
            call_count += 1
            return None  # no events yet

        async def mock_producer_failed():
            return "failed"

        trigger.get_xcom_val = mock_get_xcom_val
        trigger._get_producer_task_status = mock_producer_failed

        await trigger._log_startup_events()

        assert call_count >= 1


@pytest.mark.integration
def test_dbt_dag_with_watcher(caplog):
    """
    Run a DbtDag using `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    caplog.set_level(logging.INFO, logger="cosmos.operators._watcher.base")

    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
        ),
        render_config=RenderConfig(emit_datasets=False),
        operator_args={"trigger_rule": "all_success", "execution_timeout": timedelta(seconds=120)},
    )
    outcome = new_test_dag(watcher_dag)
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 23

    assert len(watcher_dag.task_dict) == 14
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = [
        "dbt_producer_watcher",
        "raw_customers_seed",
        "raw_orders_seed",
        "raw_payments_seed",
        "stg_customers.run",
        "stg_customers.test",
        "stg_orders.run",
        "stg_orders.test",
        "stg_payments.run",
        "stg_payments.test",
        "customers.run",
        "customers.test",
        "orders.run",
        "orders.test",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(watcher_dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_customers_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_orders_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["raw_payments_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_customers.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_orders.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_payments.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["customers.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["orders.run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_customers.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_orders.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["stg_payments.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["customers.test"], DbtTestWatcherOperator)
    assert isinstance(watcher_dag.task_dict["orders.test"], DbtTestWatcherOperator)

    assert watcher_dag.task_dict["dbt_producer_watcher"].downstream_task_ids == {
        "raw_payments_seed",
        "raw_orders_seed",
        "raw_customers_seed",
    }

    assert (
        '''"node_status": "success", "resource_type": "seed", "unique_id": "seed.jaffle_shop.raw_orders"'''
        not in caplog.text
    )

    log_message = "OK loaded seed file public.raw_orders"
    assert log_message in caplog.text

    # Verify that log messages are not duplicated (each dbt message should appear only once)
    message_count = caplog.text.count(log_message)
    assert message_count == 1, f"Expected '{log_message}' to be logged exactly once, but found {message_count} times"


@pytest.mark.integration
def test_dbt_dag_with_watcher_and_group_nodes_by_folder(capsys):
    """
    Run a DbtDag using ExecutionMode.WATCHER with RenderConfig(group_nodes_by_folder=True)
    and TestBehavior.AFTER_ALL (mirrors multi_folder_grouped_watcher_dag from dev/dags).
    """
    watcher_dag = DbtDag(
        project_config=ProjectConfig(dbt_project_path=MULTI_FOLDER_DBT_PROJ_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
        render_config=RenderConfig(
            group_nodes_by_folder=True,
            test_behavior=TestBehavior.AFTER_ALL,
            emit_datasets=False,
        ),
        operator_args={
            "install_deps": True,
            "trigger_rule": "all_success",
            "execution_timeout": timedelta(seconds=120),
        },
        start_date=datetime(2024, 1, 1),
        dag_id="multi_folder_grouped_watcher_dag",
        default_args={"retries": 0},
    )
    outcome = new_test_dag(watcher_dag)
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 6  # 3 seeds + 3 models
    task_ids = set(watcher_dag.task_dict)
    # 1 producer + 3 seeds + 3 model runs + 1 after_all test = 8
    assert len(task_ids) == 8
    assert "dbt_producer_watcher" in task_ids
    assert "seeds.seeds_a.products_seed" in task_ids
    assert "seeds.seeds_b.regions_seed" in task_ids
    assert "seeds.seeds_b.region_managers_seed" in task_ids
    assert "models.models_a.stg_products_run" in task_ids
    assert "models.models_a.dim_products_run" in task_ids
    assert "models.models_b.stg_regions_run" in task_ids
    assert "multi_folder_test" in task_ids

    assert isinstance(watcher_dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict["seeds.seeds_a.products_seed"], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict["models.models_a.stg_products_run"], DbtRunWatcherOperator)

    # AFTER_ALL test task is rendered as DbtTestLocalOperator, not DbtTestWatcherOperator
    from cosmos.operators.local import DbtTestLocalOperator

    assert isinstance(watcher_dag.task_dict["multi_folder_test"], DbtTestLocalOperator)

    # Check dependencies
    assert watcher_dag.task_dict["dbt_producer_watcher"].downstream_task_ids == {
        "seeds.seeds_b.regions_seed",
        "seeds.seeds_a.products_seed",
        "seeds.seeds_b.region_managers_seed",
    }
    assert watcher_dag.task_dict["seeds.seeds_a.products_seed"].downstream_task_ids == {
        "models.models_a.stg_products_run"
    }
    assert watcher_dag.task_dict["seeds.seeds_b.regions_seed"].downstream_task_ids == {
        "models.models_b.stg_regions_run"
    }
    assert watcher_dag.task_dict["seeds.seeds_b.region_managers_seed"].downstream_task_ids == {
        "models.models_b.stg_regions_run"
    }


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_dag_with_watcher_and_subprocess(caplog):
    """
    Run a DbtDag using `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag_with_subprocess",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(emit_datasets=False, select=["raw_orders"], test_behavior=TestBehavior.AFTER_ALL),
        operator_args={"trigger_rule": "all_success", "execution_timeout": timedelta(seconds=120)},
    )
    dag_run = new_test_dag(watcher_dag)
    assert dag_run.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 1

    assert len(watcher_dag.task_dict) == 3
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = ["dbt_producer_watcher", "raw_orders_seed", "jaffle_shop_test"]
    assert tasks_names == expected_task_names
    # Confirm that the dbt command was successfully run using the given dbt executable path:
    assert "venv-subprocess/bin/dbt'), 'build'" in caplog.text
    # Confirm that the seed was successfully run and the log output was JSON:
    assert (
        '''"node_status": "success", "resource_type": "seed", "unique_id": "seed.jaffle_shop.raw_orders"'''
        not in caplog.text
    )

    log_message = "OK loaded seed file public.raw_orders"
    assert log_message in caplog.text

    # Verify that log messages are not duplicated (each dbt message should appear only once)
    message_count = sum(1 for record in caplog.records if log_message in record.message)
    assert message_count == 1, f"Expected '{log_message}' to be logged exactly once, but found {message_count} times"


# Airflow 3.0.0 hangs indefinitely, while Airflow 3.0.6 fails due to this Airflow bug:
# https://github.com/apache/airflow/issues/51816
@pytest.mark.skipif(
    AIRFLOW_VERSION == Version("3.0"),
    reason="Airflow hangs in these versions when trying to fetch XCom from the triggerer when using dags.test()",
)
@pytest.mark.integration
def test_dbt_dag_with_watcher_and_empty_model(caplog):
    """
    Run a DbtDag using `ExecutionMode.WATCHER` and a dbt project with an empty model. This was a situation observed by an Astronomer customer.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_WITH_EMPTY_MODEL_PATH,
    )
    # There are two dbt projects defined in this folder.
    # When we run `dbt ls`, we can see this:
    #
    # 10:32:30  Found 2 models, 464 macros
    # micro_dbt_project.add_row
    # micro_dbt_project.empty_model
    #
    # However, during `dbt build`, dbt skips running the empty model, and only runs the add_row model:
    #
    # 10:29:03  Running with dbt=1.11.2
    # 10:29:03  Registered adapter: postgres=1.10.0
    # 10:29:03  Found 2 models, 464 macros
    # 10:29:03
    # 10:29:03  Concurrency: 4 threads (target='dev')
    # 10:29:03
    # 10:29:03  1 of 1 START sql view model public.add_row ..................................... [RUN]
    # 10:29:03  1 of 1 OK created sql view model public.add_row ................................ [CREATE VIEW in 0.06s]
    # 10:29:03
    # 10:29:03  Finished running 1 view model in 0 hours 0 minutes and 0.19 seconds (0.19s).
    # 10:29:03
    # 10:29:03  Completed successfully
    # 10:29:03
    # 10:29:03  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1

    caplog.set_level(logging.DEBUG, logger="cosmos.operators._watcher.base")

    watcher_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="watcher_dag_empty_model",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
        ),
        render_config=RenderConfig(emit_datasets=False, test_behavior=TestBehavior.NONE),
        operator_args={
            "trigger_rule": "all_success",
            "execution_timeout": timedelta(seconds=10),
        },
        dagrun_timeout=timedelta(seconds=30),
    )
    outcome = new_test_dag(watcher_dag)
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 2

    assert len(watcher_dag.task_dict) == 3
    tasks_names = [task.task_id for task in watcher_dag.topological_sort()]
    expected_task_names = [
        "dbt_producer_watcher",
        "add_row_run",
        "empty_model_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(watcher_dag.task_dict["dbt_producer_watcher"], DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict["add_row_run"], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict["empty_model_run"], DbtRunWatcherOperator)

    assert watcher_dag.task_dict["dbt_producer_watcher"].downstream_task_ids == {
        "add_row_run",
        "empty_model_run",
    }

    assert "Total filtered nodes: 2" in caplog.text
    assert "Finished running node model.micro_dbt_project.add_row" in caplog.text
    assert "Finished running node model.micro_dbt_project.empty_model_run" not in caplog.text
    assert "Model 'model.micro_dbt_project.empty_model' was skipped by the dbt command" in caplog.text


@pytest.mark.integration
def test_dbt_task_group_with_watcher():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
    from airflow import DAG

    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
    }

    with DAG(
        dag_id="example_watcher_taskgroup",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher:
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        pre_dbt = EmptyOperator(task_id="pre_dbt")

        dbt_task_group = DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

        pre_dbt
        dbt_task_group

    # Unfortunately, due to a bug in Airflow, we are not being able to set the producer task as an upstream task of the other TaskGroup tasks:
    # https://github.com/apache/airflow/issues/56723
    # When we run dag.test(), non-producer tasks are being executed before the producer task was scheduled.
    # For this reason, we are commenting out these two lines for now:
    # outcome = dag_dbt_task_group_watcher.test()
    # assert outcome.state == DagRunState.SUCCESS
    # Fortunately, when we trigger the DAG run manually, the weight is being respected and the producer task is being picked up in advance.

    assert len(dag_dbt_task_group_watcher.task_dict) == 10
    tasks_names = [task.task_id for task in dag_dbt_task_group_watcher.topological_sort()]

    expected_task_names = [
        "pre_dbt",
        "dbt_task_group.dbt_producer_watcher",
        "dbt_task_group.raw_customers_seed",
        "dbt_task_group.raw_orders_seed",
        "dbt_task_group.raw_payments_seed",
        "dbt_task_group.stg_customers_run",
        "dbt_task_group.stg_orders_run",
        "dbt_task_group.stg_payments_run",
        "dbt_task_group.customers_run",
        "dbt_task_group.orders_run",
    ]
    assert tasks_names == expected_task_names

    assert isinstance(
        dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"], DbtProducerWatcherOperator
    )
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_customers_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_orders_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.raw_payments_seed"], DbtSeedWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_customers_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_orders_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.stg_payments_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.customers_run"], DbtRunWatcherOperator)
    assert isinstance(dag_dbt_task_group_watcher.task_dict["dbt_task_group.orders_run"], DbtRunWatcherOperator)

    assert dag_dbt_task_group_watcher.task_dict["dbt_task_group.dbt_producer_watcher"].downstream_task_ids == set()


@pytest.mark.integration
def test_dbt_task_group_with_watcher_has_correct_dbt_cmd():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm that the dbt command flags include the expected flags.
    """
    from airflow import DAG

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    context = {"ti": MagicMock(), "run_id": "test_run_id"}

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
        "full_refresh": True,
    }

    with DAG(
        dag_id="example_watcher_taskgroup_flags",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher_flags:
        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

    producer_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.dbt_producer_watcher"]
    assert producer_operator.base_cmd == ["build"]

    cmd_flags = producer_operator.add_cmd_flags()

    # Build the command without executing it
    full_cmd, env = producer_operator.build_cmd(context=context, cmd_flags=cmd_flags)

    # Verify the command was built correctly
    assert full_cmd[1] == "build"  # dbt build command
    assert "--full-refresh" in full_cmd


@pytest.mark.integration
def test_dbt_task_group_with_watcher_has_correct_templated_dbt_cmd():
    """
    Create an Airflow DAG that uses a DbtTaskGroup with `ExecutionMode.WATCHER`.
    Confirm that the dbt commands for both producer and sensor tasks include the expected templated flags.
    """
    from airflow import DAG

    from cosmos import DbtTaskGroup, ExecutionConfig
    from cosmos.config import RenderConfig
    from cosmos.constants import ExecutionMode, TestBehavior

    context = {"ti": MagicMock(try_number=1), "run_id": "test_run_id"}

    operator_args = {
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "execution_timeout": timedelta(seconds=120),
        "full_refresh": True,
        "dbt_cmd_flags": ["--threads", "{{ 1 if ti.try_number > 1 else 'x' }}"],
    }

    with DAG(
        dag_id="example_watcher_taskgroup_flags",
        start_date=datetime(2025, 1, 1),
    ) as dag_dbt_task_group_watcher_flags:
        """
        Example DAG using a DbtTaskGroup with ExecutionMode.WATCHER, validating that templated dbt command
        flags are rendered and passed correctly to both producer and sensor tasks.
        """
        DbtTaskGroup(
            group_id="dbt_task_group",
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.WATCHER,
            ),
            profile_config=profile_config,
            project_config=project_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
            operator_args=operator_args,
        )

    # Basic check for producer task
    producer_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.dbt_producer_watcher"]
    producer_operator.render_template_fields(context=context)  # Render the templated fields
    assert producer_operator.base_cmd == ["build"]

    # Build the command without executing it and verify it was built correctly
    cmd_flags = producer_operator.add_cmd_flags()
    full_cmd, _ = producer_operator.build_cmd(context=context, cmd_flags=cmd_flags)
    assert full_cmd[1] == "build"  # dbt build command

    cmd = " ".join(full_cmd)
    assert "--full-refresh" in full_cmd
    assert "--threads x" in cmd

    # Setup for checking the sensor task, which has templated command flags
    context["ti"].task.dag.get_task.return_value = producer_operator
    context["ti"].try_number = 2
    sensor_operator = dag_dbt_task_group_watcher_flags.task_dict["dbt_task_group.stg_customers_run"]
    sensor_operator.render_template_fields(context=context)  # Render the templated fields
    assert sensor_operator.base_cmd == ["run"]

    # Build the command without executing it and verify it was built correctly
    cmd_flags = sensor_operator.add_cmd_flags()
    full_cmd, _ = sensor_operator.build_cmd(context=context, cmd_flags=cmd_flags)
    assert full_cmd[1] == "run"  # dbt run command

    cmd = " ".join(full_cmd)
    assert "--select fqn:jaffle_shop.staging.stg_customers" in cmd
    assert "--threads 1" in cmd


@pytest.mark.integration
def test_sensor_and_producer_different_param_values(mock_bigquery_conn):
    profile_mapping = get_automatic_profile_mapping(mock_bigquery_conn.conn_id, {})
    _profile_config = ProfileConfig(
        profile_name="airflow_db",
        target_name="bq",
        profile_mapping=profile_mapping,
    )
    dbt_project_path = Path(__file__).parent.parent.parent / "dev/dags/dbt"

    dag = DbtDag(
        project_config=ProjectConfig(dbt_project_path=dbt_project_path / "jaffle_shop"),
        profile_config=_profile_config,
        operator_args={
            "install_deps": True,
            "full_refresh": True,
            "deferrable": False,
            "execution_timeout": timedelta(seconds=1),
        },
        render_config=RenderConfig(test_behavior=TestBehavior.NONE),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER, setup_operator_args={"execution_timeout": timedelta(seconds=2)}
        ),
        schedule="@daily",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        dag_id="test_sensor_args_import",
    )

    for task in dag.tasks_map.values():
        if isinstance(task, DbtProducerWatcherOperator):
            assert task.execution_timeout == timedelta(seconds=2)
        else:
            assert task.execution_timeout == timedelta(seconds=1)


def test_dbt_source_watcher_operator_template_fields():
    """Test that DbtSourceWatcherOperator doesn't include model_unique_id in template_fields."""
    from cosmos.operators.local import DbtSourceLocalOperator
    from cosmos.operators.watcher import DbtSourceWatcherOperator

    # DbtSourceWatcherOperator should NOT have model_unique_id in template_fields
    # because it runs locally and doesn't watch models, it executes source freshness
    assert "model_unique_id" not in DbtSourceWatcherOperator.template_fields

    # DbtSourceWatcherOperator should inherit template_fields from DbtSourceLocalOperator
    assert DbtSourceWatcherOperator.template_fields == DbtSourceLocalOperator.template_fields


class TestDbtTestWatcherOperator:
    """Tests for DbtTestWatcherOperator — the sensor that watches aggregated test results."""

    MODEL_UID = "model.jaffle_shop.stg_orders"
    TESTS_STATUS_XCOM_KEY = "model__jaffle_shop__stg_orders_tests_status"

    def make_sensor(self, **overrides):
        extra_context = {"dbt_node_config": {"unique_id": self.MODEL_UID}}
        sensor = DbtTestWatcherOperator(
            task_id="stg_orders.test",
            project_dir="/tmp/project",
            profile_config=None,
            deferrable=overrides.pop("deferrable", True),
            extra_context=extra_context,
            **overrides,
        )
        sensor._get_producer_task_status = MagicMock(return_value=None)
        return sensor

    def make_context(self, ti_mock, *, run_id="test-run", map_index=0):
        return {
            "ti": ti_mock,
            "run_id": run_id,
            "task_instance": MagicMock(map_index=map_index),
        }

    def test_is_test_sensor_returns_true(self):
        sensor = self.make_sensor()
        assert sensor.is_test_sensor is True

    def test_poke_returns_true_when_tests_pass(self):
        """When the aggregated test status is 'pass', poke should return True."""
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = "pass"
        context = self.make_context(ti)

        assert sensor.poke(context) is True

    def test_poke_raises_when_tests_fail(self):
        """When the aggregated test status is 'fail', poke should raise AirflowException."""
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = "fail"
        context = self.make_context(ti)

        with pytest.raises(AirflowException, match="Tests for model"):
            sensor.poke(context)

    def test_poke_returns_false_when_no_status_yet(self):
        """When the aggregated test status has not been pushed yet, poke should return False."""
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        assert sensor.poke(context) is False

    def test_poke_reads_correct_xcom_key(self):
        """Poke should pull from the _tests_status XCom key, not the regular _status key."""
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = "pass"
        context = self.make_context(ti)

        sensor.poke(context)

        # Verify xcom_pull was called with the aggregated tests_status key
        calls = ti.xcom_pull.call_args_list
        xcom_keys_used = [call.kwargs.get("key") or call[1].get("key") for call in calls]
        assert self.TESTS_STATUS_XCOM_KEY in xcom_keys_used

    def test_fallback_raises_on_retry(self):
        """On retry (try_number > 1), the test sensor should raise since test re-execution is not yet supported."""
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 2
        context = self.make_context(ti)

        with pytest.raises(AirflowException, match="Test re-execution is not yet supported"):
            sensor.poke(context)


# ---------------------------------------------------------------------------
# Tests for source freshness feature (_check_source_freshness=True)
# ---------------------------------------------------------------------------


def _make_dbt_node(unique_id: str, resource_type: DbtResourceType, depends_on: list[str]) -> DbtNode:
    return DbtNode(
        unique_id=unique_id,
        resource_type=resource_type,
        depends_on=depends_on,
        path_base=Path("/tmp"),
        original_file_path=Path("/tmp/fake.sql"),
    )


class TestDefaultFreshnessCallback:
    """Unit tests for the _default_freshness_callback helper."""

    SOURCE_ID = "source.proj.raw.orders"
    MODEL_ID = "model.proj.stg_orders"
    MODEL_ID_2 = "model.proj.orders"
    TEST_ID = "test.proj.not_null_stg_orders_id.abc123"

    def test_returns_empty_when_nodes_is_none(self):
        result_ids, status = _default_freshness_callback(None, None, None, None, {"results": []})
        assert result_ids == []
        assert status == "skip"

    def test_returns_empty_when_sources_json_is_none(self):
        nodes = {self.MODEL_ID: _make_dbt_node(self.MODEL_ID, DbtResourceType.MODEL, [self.SOURCE_ID])}
        result_ids, status = _default_freshness_callback(None, None, None, nodes, None)
        assert result_ids == []
        assert status == "skip"

    def test_returns_empty_when_no_stale_sources(self):
        nodes = {self.MODEL_ID: _make_dbt_node(self.MODEL_ID, DbtResourceType.MODEL, [self.SOURCE_ID])}
        sources_json = {"results": [{"unique_id": self.SOURCE_ID, "status": "pass"}]}
        result_ids, status = _default_freshness_callback(None, None, None, nodes, sources_json)
        assert result_ids == []
        assert status == "skip"

    def test_returns_direct_dependent_model(self):
        nodes = {
            self.SOURCE_ID: _make_dbt_node(self.SOURCE_ID, DbtResourceType.SOURCE, []),
            self.MODEL_ID: _make_dbt_node(self.MODEL_ID, DbtResourceType.MODEL, [self.SOURCE_ID]),
        }
        sources_json = {"results": [{"unique_id": self.SOURCE_ID, "status": "error"}]}
        result_ids, status = _default_freshness_callback(None, None, None, nodes, sources_json)
        assert result_ids == [self.MODEL_ID]
        assert status == "skip"

    def test_returns_transitive_dependents(self):
        # source → model_1 → model_2
        model_1 = "model.proj.stg_orders"
        model_2 = "model.proj.orders"
        nodes = {
            self.SOURCE_ID: _make_dbt_node(self.SOURCE_ID, DbtResourceType.SOURCE, []),
            model_1: _make_dbt_node(model_1, DbtResourceType.MODEL, [self.SOURCE_ID]),
            model_2: _make_dbt_node(model_2, DbtResourceType.MODEL, [model_1]),
        }
        sources_json = {"results": [{"unique_id": self.SOURCE_ID, "status": "warn"}]}
        result_ids, status = _default_freshness_callback(None, None, None, nodes, sources_json)
        assert sorted(result_ids) == sorted([model_1, model_2])
        assert status == "skip"

    def test_test_nodes_are_filtered_out(self):
        """Test nodes that depend on a stale source must not be returned — dbt skips them automatically."""
        nodes = {
            self.SOURCE_ID: _make_dbt_node(self.SOURCE_ID, DbtResourceType.SOURCE, []),
            self.MODEL_ID: _make_dbt_node(self.MODEL_ID, DbtResourceType.MODEL, [self.SOURCE_ID]),
            self.TEST_ID: _make_dbt_node(self.TEST_ID, DbtResourceType.TEST, [self.MODEL_ID]),
        }
        sources_json = {"results": [{"unique_id": self.SOURCE_ID, "status": "error"}]}
        result_ids, status = _default_freshness_callback(None, None, None, nodes, sources_json)
        assert self.TEST_ID not in result_ids
        assert self.MODEL_ID in result_ids

    def test_unrelated_models_not_included(self):
        unrelated = "model.proj.unrelated"
        nodes = {
            self.SOURCE_ID: _make_dbt_node(self.SOURCE_ID, DbtResourceType.SOURCE, []),
            self.MODEL_ID: _make_dbt_node(self.MODEL_ID, DbtResourceType.MODEL, [self.SOURCE_ID]),
            unrelated: _make_dbt_node(unrelated, DbtResourceType.MODEL, []),
        }
        sources_json = {"results": [{"unique_id": self.SOURCE_ID, "status": "error"}]}
        result_ids, _ = _default_freshness_callback(None, None, None, nodes, sources_json)
        assert unrelated not in result_ids
        assert self.MODEL_ID in result_ids


class TestProducerSourceFreshness:
    """Unit tests for DbtProducerWatcherOperator source-freshness logic."""

    SOURCE_ID = "source.proj.raw.orders"
    MODEL_ID = "model.proj.stg_orders"
    MODEL_NAME = "stg_orders"

    def _make_op(self, **kwargs: Any) -> DbtProducerWatcherOperator:
        return DbtProducerWatcherOperator(
            project_dir="/tmp/proj",
            profile_config=None,
            **kwargs,
        )

    def test_run_source_freshness_sets_base_cmd_and_restores_it(self):
        op = self._make_op()
        original_base_cmd = list(op.base_cmd)
        context: Any = {"ti": MagicMock(), "run_id": "test_run"}
        with (
            patch.object(op, "build_cmd", return_value=(["dbt", "source", "freshness"], {})) as mock_build,
            patch.object(op, "run_command") as mock_run,
        ):
            op._run_source_freshness(context)
        mock_build.assert_called_once_with(context=context, cmd_flags=["--log-format", "json"])
        mock_run.assert_called_once_with(cmd=["dbt", "source", "freshness"], env={}, context=context)
        assert op.base_cmd == original_base_cmd

    def test_run_source_freshness_restores_base_cmd_on_error(self):
        op = self._make_op()
        original_base_cmd = list(op.base_cmd)
        context: Any = {"ti": MagicMock(), "run_id": "test_run"}
        with (
            patch.object(op, "build_cmd", return_value=([], {})),
            patch.object(op, "run_command", side_effect=RuntimeError("dbt error")),
            pytest.raises(RuntimeError),
        ):
            op._run_source_freshness(context)
        assert op.base_cmd == original_base_cmd

    def test_push_skipped_xcom_pushes_status_key(self):
        """Both invocation modes now use the unified {uid}_status XCom key."""
        op = self._make_op()
        mock_ti = MagicMock()
        with patch("cosmos.operators.watcher.safe_xcom_push") as mock_push:
            op._push_skipped_xcom_for_model(mock_ti, self.MODEL_ID)
        mock_push.assert_called_once_with(
            task_instance=mock_ti,
            key=f"{self.MODEL_ID.replace('.', '__')}_status",
            value="skipped",
        )

    def test_skipped_node_token_pushes_xcom_and_updates_exclude(self):
        op = self._make_op()
        context: Any = {"ti": MagicMock()}
        with patch.object(op, "_push_skipped_xcom_for_model") as mock_push:
            op._skipped_node_token(context, [self.MODEL_ID])
        mock_push.assert_called_once_with(context["ti"], self.MODEL_ID)
        assert self.MODEL_NAME in op.exclude

    def test_skipped_node_token_noop_on_empty_list(self):
        op = self._make_op()
        context: Any = {"ti": MagicMock()}
        with patch.object(op, "_push_skipped_xcom_for_model") as mock_push:
            op._skipped_node_token(context, [])
        mock_push.assert_not_called()
        assert not (op.exclude or "").strip()

    def test_skipped_node_token_merges_with_existing_exclude(self):
        op = self._make_op()
        op.exclude = "already_excluded"
        context: Any = {"ti": MagicMock()}
        with patch.object(op, "_push_skipped_xcom_for_model"):
            op._skipped_node_token(context, [self.MODEL_ID])
        assert "already_excluded" in op.exclude
        assert self.MODEL_NAME in op.exclude

    @pytest.mark.parametrize(
        "existing_exclude",
        [
            ["already_excluded"],
            {"already_excluded"},
            ("already_excluded",),
        ],
        ids=["list", "set", "tuple"],
    )
    def test_skipped_node_token_merges_with_list_set_tuple_exclude(self, existing_exclude: Any):
        """When existing exclude is a list, set, or tuple it should be merged with the new model name."""
        op = self._make_op()
        op.exclude = existing_exclude
        context: Any = {"ti": MagicMock()}
        with patch.object(op, "_push_skipped_xcom_for_model"):
            op._skipped_node_token(context, [self.MODEL_ID])
        assert "already_excluded" in op.exclude
        assert self.MODEL_NAME in op.exclude

    def test_apply_source_freshness_calls_callback_and_skips_nodes(self):
        from unittest.mock import PropertyMock

        op = self._make_op()
        context: Any = {"ti": MagicMock()}
        mock_dag = MagicMock()
        with (
            patch.object(type(op), "dag", new_callable=PropertyMock, return_value=mock_dag),
            patch.object(op, "_run_source_freshness"),
            patch.object(op, "_freshness_callback", return_value=([self.MODEL_ID], "skip")),
            patch.object(op, "_skipped_node_token") as mock_skip,
        ):
            op._apply_source_freshness(context)
        mock_skip.assert_called_once_with(context, [self.MODEL_ID])

    def test_apply_source_freshness_noop_when_callback_returns_empty(self):
        from unittest.mock import PropertyMock

        op = self._make_op()
        context: Any = {"ti": MagicMock()}
        with (
            patch.object(type(op), "dag", new_callable=PropertyMock, return_value=MagicMock()),
            patch.object(op, "_run_source_freshness"),
            patch.object(op, "_freshness_callback", return_value=([], "skip")),
            patch.object(op, "_skipped_node_token") as mock_skip,
        ):
            op._apply_source_freshness(context)
        mock_skip.assert_not_called()

    def test_execute_skips_apply_when_flag_disabled(self):
        """_apply_source_freshness must not be called when _check_source_freshness is False."""
        op = self._make_op(_check_source_freshness=False)
        context: Any = {"ti": MagicMock(try_number=1), "run_id": "r1"}
        with (
            patch.object(op, "_apply_source_freshness") as mock_apply,
            patch("cosmos.operators.local.DbtLocalBaseOperator.execute", return_value=None),
        ):
            op.execute(context)
        mock_apply.assert_not_called()

    def test_execute_calls_apply_when_flag_enabled(self):
        """_apply_source_freshness must be called when _check_source_freshness is True."""
        op = self._make_op(_check_source_freshness=True)
        context: Any = {"ti": MagicMock(try_number=1), "run_id": "r1"}
        with (
            patch.object(op, "_apply_source_freshness") as mock_apply,
            patch("cosmos.operators.local.DbtLocalBaseOperator.execute", return_value=None),
        ):
            op.execute(context)
        mock_apply.assert_called_once_with(context)
