import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

# Prevent boto3 from trying to resolve credentials or region from the environment
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-central-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

from cosmos.constants import _DBT_STARTUP_EVENTS_XCOM_KEY
from cosmos.operators.watcher_aws_ecs import (
    DbtBuildWatcherAwsEcsOperator,
    DbtConsumerWatcherAwsEcsSensor,
    DbtProducerWatcherAwsEcsOperator,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

OPERATOR_ARGS = {
    "aws_conn_id": "my-aws-conn-id",
    "cluster": "test-cluster",
    "task_definition": "test-task",
    "container_name": "dbt",
    "overrides": {},
    "awslogs_group": "/ecs/dbt",
    "awslogs_stream_prefix": "ecs/dbt",
    "awslogs_region": "eu-central-1",
    "project_dir": "/tmp/project",
    "profile_args": {},
}


def make_producer(**kwargs):
    return DbtProducerWatcherAwsEcsOperator(
        **{**OPERATOR_ARGS, **kwargs},
    )


def make_sensor(**kwargs):
    extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
    kwargs["extra_context"] = extra_context
    sensor = DbtConsumerWatcherAwsEcsSensor(
        task_id="model.my_model",
        **{**OPERATOR_ARGS, **kwargs},
    )
    sensor._get_producer_task_status = MagicMock(return_value=None)
    return sensor


def make_context(ti_mock, *, run_id: str = "test-run", map_index: int = 0):
    return {
        "ti": ti_mock,
        "run_id": run_id,
        "task_instance": MagicMock(map_index=map_index),
    }


# ---------------------------------------------------------------------------
# DbtProducerWatcherAwsEcsOperator -- __init__ invariants
# ---------------------------------------------------------------------------


def test_retries_set_to_zero_on_init():
    op = make_producer()
    assert op.retries == 0


def test_retries_overridden_even_if_user_sets_them():
    op = make_producer(retries=5)
    assert op.retries == 0


def test_deferrable_always_false():
    op = make_producer(deferrable=True)
    assert op.deferrable is False


def test_log_format_json_appended():
    op = make_producer()
    assert "--log-format" in op.dbt_cmd_flags
    idx = op.dbt_cmd_flags.index("--log-format")
    assert op.dbt_cmd_flags[idx + 1] == "json"


def test_aws_logs_enabled_true_with_valid_params():
    """If _aws_logs_enabled() returns False, _get_task_log_fetcher is never called."""
    op = make_producer()
    assert op._aws_logs_enabled()


# ---------------------------------------------------------------------------
# DbtProducerWatcherAwsEcsOperator -- execute retry guard
# ---------------------------------------------------------------------------


@patch("cosmos.operators.aws_ecs.DbtBuildAwsEcsOperator.execute")
def test_skips_retry_attempt(mock_execute, caplog):
    op = make_producer()

    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti}

    with caplog.at_level(logging.INFO):
        result = op.execute(context=context)

    mock_execute.assert_not_called()
    assert result is None
    assert any("does not support Airflow retries" in m for m in caplog.messages)
    assert any("skipping execution" in m for m in caplog.messages)


def test_raises_exception_when_task_instance_missing():
    op = make_producer()
    with pytest.raises(AirflowException, match="task instance"):
        op.execute(context={"ti": None})


# ---------------------------------------------------------------------------
# DbtProducerWatcherAwsEcsOperator -- build_and_run_cmd validation
# ---------------------------------------------------------------------------


def test_get_task_log_fetcher_raises_without_awslogs_group():
    # _get_task_log_fetcher contains the validation; call it directly since
    # EcsRunTaskOperator.execute only calls it when _aws_logs_enabled() is True,
    # which requires awslogs_group to already be set.
    op = make_producer()
    op.awslogs_group = None
    with pytest.raises(AirflowException, match="awslogs_group"):
        op._get_task_log_fetcher()


def test_get_task_log_fetcher_raises_without_awslogs_stream_prefix():
    op = make_producer()
    op.awslogs_stream_prefix = None
    with pytest.raises(AirflowException, match="awslogs_stream_prefix"):
        op._get_task_log_fetcher()


# ---------------------------------------------------------------------------
# DbtBuildWatcherAwsEcsOperator -- NotImplementedError
# ---------------------------------------------------------------------------


def test_dbt_build_watcher_aws_ecs_operator_raises_not_implemented_error():
    expected_message = "`ExecutionMode.WATCHER` does not expose a DbtBuild operator"
    with pytest.raises(NotImplementedError, match=expected_message):
        DbtBuildWatcherAwsEcsOperator()


# ---------------------------------------------------------------------------
# DbtConsumerWatcherAwsEcsSensor
# ---------------------------------------------------------------------------


def test_use_event_returns_false():
    sensor = make_sensor()
    assert sensor.use_event() is False


def test_first_execution_behaves_as_base_consumer_sensor():
    sensor = make_sensor()

    ti = MagicMock()
    ti.try_number = 1

    # xcom_pull is called twice: once for startup events (expects list of dicts)
    # and once for node status (expects a string). Return the right type per key.
    def xcom_pull_side_effect(*args, **kwargs):
        if kwargs.get("key") == _DBT_STARTUP_EVENTS_XCOM_KEY:
            return []
        return "success"

    ti.xcom_pull.side_effect = xcom_pull_side_effect
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    ti.xcom_pull.assert_called()


@patch("cosmos.operators.aws_ecs.DbtRunAwsEcsOperator.build_and_run_cmd")
def test_retry_executes_as_dbt_run_aws_ecs_operator(mock_build_and_run_cmd):
    sensor = make_sensor()

    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--threads", "2"]
    context = make_context(ti)

    result = sensor.poke(context)

    assert result is True
    mock_build_and_run_cmd.assert_called_once()


# ---------------------------------------------------------------------------
# DbtProducerWatcherAwsEcsOperator -- log fetcher integration
# ---------------------------------------------------------------------------


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_get_task_log_fetcher_returns_dbt_aws_task_log_fetcher(mock_store):
    """_get_task_log_fetcher returns a DbtAwsTaskLogFetcher (not the stock one)."""
    from cosmos.operators.watcher_aws_ecs import DbtAwsTaskLogFetcher

    op = make_producer()
    op._current_context = make_context(MagicMock())

    fetcher = op._get_task_log_fetcher()

    assert isinstance(fetcher, DbtAwsTaskLogFetcher)


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_on_log_line_callback_calls_store_dbt_resource_status(mock_store):
    """The on_log_line closure produced by _get_task_log_fetcher delegates to
    store_dbt_resource_status_from_log with the correct context shape."""
    op = make_producer()
    context = make_context(MagicMock())
    op._current_context = context

    with patch.object(op, "_get_logs_stream_name", return_value="ecs/dbt/task-123"):
        fetcher = op._get_task_log_fetcher()

    log_line = '{"info": {"name": "NodeFinished"}, "data": {"node_info": {"unique_id": "model.x.y", "node_status": "success"}}}'
    fetcher.on_log_line(log_line)

    mock_store.assert_called_once_with(
        log_line,
        {"context": context, "project_dir": op.project_dir},
    )


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_on_log_line_called_for_each_log_event(mock_store):
    """on_log_line is invoked once per log event emitted by the fetcher's run loop."""

    op = make_producer()
    op._current_context = make_context(MagicMock())

    # with patch.object(op, "_get_logs_stream_name", return_value="ecs/dbt/task-123"):
    fetcher = op._get_task_log_fetcher()

    fake_events = [
        {"message": "line-one"},
        {"message": "line-two"},
        {"message": "line-three"},
    ]

    stop_flag = {"count": 0}

    def is_stopped():
        stop_flag["count"] += 1
        return stop_flag["count"] > 1  # run the loop body exactly once

    with (
        patch.object(fetcher, "is_stopped", side_effect=is_stopped),
        patch.object(fetcher, "_get_log_events", return_value=iter(fake_events)),
        patch("time.sleep"),
    ):
        fetcher.run()

    assert mock_store.call_count == 3
    called_messages = [call.args[0] for call in mock_store.call_args_list]
    assert called_messages == ["line-one", "line-two", "line-three"]


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_build_and_run_cmd_stashes_context_before_ecs_execute(mock_store):
    """build_and_run_cmd must populate _current_context before the ECS operator
    calls _get_task_log_fetcher, which depends on it."""
    op = make_producer()
    context = make_context(MagicMock())

    captured = {}

    def fake_super_build_and_run_cmd(ctx, **kwargs):
        # Simulate ECS internals calling _get_task_log_fetcher mid-execute.
        captured["context_at_call_time"] = op._current_context

    with patch(
        "cosmos.operators.aws_ecs.DbtBuildAwsEcsOperator.build_and_run_cmd",
        side_effect=fake_super_build_and_run_cmd,
    ):
        op.build_and_run_cmd(context)

    assert captured["context_at_call_time"] is context


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_build_and_run_cmd_forwards_log_lines_to_store(mock_store):
    op = make_producer()
    context = make_context(MagicMock())

    fake_log_line = '{"info": {"name": "NodeFinished"}, "data": {"node_info": {"unique_id": "model.x.y", "node_status": "success"}}}'

    stop_calls = {"count": 0}

    def is_stopped_after_one_iteration():
        stop_calls["count"] += 1
        return stop_calls["count"] > 1

    with (
        patch.object(op, "_start_task"),
        patch.object(op, "_wait_for_task_ended"),
        patch(
            "cosmos.operators.watcher_aws_ecs.DbtAwsTaskLogFetcher.is_stopped",
            side_effect=is_stopped_after_one_iteration,
        ),
        patch(
            "cosmos.operators.watcher_aws_ecs.DbtAwsTaskLogFetcher._get_log_events",
            return_value=iter([{"message": fake_log_line}]),
        ),
        patch("cosmos.operators.watcher_aws_ecs.DbtAwsTaskLogFetcher.get_last_log_message"),
        patch("time.sleep"),
    ):
        op.build_and_run_cmd(context)

    mock_store.assert_called_once_with(
        fake_log_line,
        {"context": context, "project_dir": op.project_dir},
    )
