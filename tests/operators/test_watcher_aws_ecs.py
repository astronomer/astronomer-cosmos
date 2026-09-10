import json
import queue
from datetime import timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon import __version__ as amazon_provider_version
from packaging.version import Version

from cosmos.airflow.graph import _WATCHER_TO_FALLBACK_EXECUTION_MODE, calculate_operator_class
from cosmos.constants import (
    _ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION,
    PRODUCER_WATCHER_TASK_ID,
    ExecutionMode,
)
from cosmos.exceptions import CosmosValueError

if Version(amazon_provider_version) < _ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION:
    pytest.skip(
        f"Watcher AWS ECS depends on apache-airflow-providers-amazon >= {_ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION}. "
        f"Current version: {amazon_provider_version}",
        allow_module_level=True,
    )
else:
    from cosmos.operators._watcher.producer import CONTEXT_KEY
    from cosmos.operators.watcher_aws_ecs import (
        DbtBuildWatcherAwsEcsOperator,
        DbtConsumerWatcherAwsEcsSensor,
        DbtProducerWatcherAwsEcsOperator,
        DbtTestWatcherAwsEcsOperator,
        WatcherEcsLogFetcher,
    )

ECS_KWARGS = {
    "cluster": "dbt-cluster",
    "task_definition": "dbt-task:3",
    "awslogs_group": "/ecs/dbt",
    "awslogs_stream_prefix": "ecs/dbt",
    "project_dir": "dags/dbt/jaffle_shop",
    "profile_config": None,
}


def node_finished(unique_id: str, status: str = "success") -> str:
    return json.dumps(
        {
            "data": {"node_info": {"unique_id": unique_id, "node_status": status, "resource_type": "model"}},
            "info": {"name": "NodeFinished", "level": "debug", "msg": ""},
        }
    )


COMMAND_COMPLETED = json.dumps({"data": {}, "info": {"name": "CommandCompleted", "level": "info", "msg": ""}})


def make_producer(**overrides):
    return DbtProducerWatcherAwsEcsOperator(**{**ECS_KWARGS, **overrides})


# ---------------------------------------------------------------------------
# graph wiring
# ---------------------------------------------------------------------------


def test_operator_classes_resolve_from_the_execution_mode():
    assert (
        calculate_operator_class(ExecutionMode.WATCHER_AWS_ECS, "DbtRun")
        == "cosmos.operators.watcher_aws_ecs.DbtRunWatcherAwsEcsOperator"
    )
    assert (
        calculate_operator_class(ExecutionMode.WATCHER_AWS_ECS, "DbtProducer")
        == "cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator"
    )


def test_after_all_tests_fall_back_to_plain_ecs():
    assert _WATCHER_TO_FALLBACK_EXECUTION_MODE[ExecutionMode.WATCHER_AWS_ECS] == ExecutionMode.AWS_ECS


# ---------------------------------------------------------------------------
# producer construction
# ---------------------------------------------------------------------------


def test_producer_default_task_id_matches_watcher_task_id():
    assert make_producer().task_id == PRODUCER_WATCHER_TASK_ID


def test_producer_honours_explicit_task_id():
    assert make_producer(task_id="custom_producer").task_id == "custom_producer"


def test_producer_forces_json_log_format_and_default_fetch_interval():
    op = make_producer()
    assert op.dbt_cmd_flags[-2:] == ["--log-format", "json"]
    assert op.awslogs_fetch_interval == timedelta(seconds=5)
    assert op.drain_timeout == timedelta(seconds=60)


@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"deferrable": True}, "does not support deferrable=True"),
        ({"wait_for_completion": False}, "requires the producer to wait for completion"),
        ({"reattach": True}, "does not support reattach=True"),
        ({"awslogs_group": None}, "requires awslogs_group and awslogs_stream_prefix"),
        ({"awslogs_stream_prefix": None}, "requires awslogs_group and awslogs_stream_prefix"),
    ],
)
def test_producer_rejects_unsupported_arguments(overrides, match):
    with pytest.raises(CosmosValueError, match=match):
        make_producer(**overrides)


def test_producer_stores_tests_per_model():
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1", "test.pkg.t2"]}
    op = make_producer(tests_per_model=tests_per_model)
    assert op._tests_per_model is tests_per_model
    assert op._test_results_per_model == {}


def test_producer_log_fetcher_is_bound_to_the_operator_state():
    op = make_producer()
    op.arn = "arn:aws:ecs:eu-west-1:123456789012:task/dbt-cluster/abcdef"
    fetcher = op._get_task_log_fetcher()
    assert isinstance(fetcher, WatcherEcsLogFetcher)
    assert fetcher.events is op._log_events
    assert fetcher.drain_timeout == op.drain_timeout
    assert fetcher.log_group == "/ecs/dbt"
    assert fetcher.log_stream_name == "ecs/dbt/abcdef"
    assert fetcher.fetch_interval == timedelta(seconds=5)


# ---------------------------------------------------------------------------
# log fetcher
# ---------------------------------------------------------------------------


def make_fetcher(**overrides):
    kwargs = {
        "events": queue.SimpleQueue(),
        "drain_timeout": timedelta(seconds=3),
        "log_group": "/ecs/dbt",
        "log_stream_name": "ecs/dbt/abcdef",
        "fetch_interval": timedelta(seconds=0),
        "logger": MagicMock(),
        "aws_conn_id": None,
    }
    kwargs.update(overrides)
    return WatcherEcsLogFetcher(**kwargs)


def test_fetcher_collect_queues_messages_and_detects_command_completed():
    fetcher = make_fetcher()
    events = [
        {"timestamp": 1_700_000_000_000, "message": node_finished("model.pkg.a")},
        {"timestamp": 1_700_000_001_000, "message": COMMAND_COMPLETED},
    ]
    with patch.object(fetcher, "_get_log_events", return_value=iter(events)) as mock_get:
        fetcher.collect()

    mock_get.assert_called_once_with(fetcher._continuation_token)
    assert fetcher.events.get_nowait() == node_finished("model.pkg.a")
    assert fetcher.events.get_nowait() == COMMAND_COMPLETED
    assert fetcher.command_completed is True
    assert fetcher.logger.info.call_count == 2


def test_fetcher_ignores_non_json_and_other_events_for_completion():
    fetcher = make_fetcher()
    events = [
        {"timestamp": 1, "message": "plain text mentioning CommandCompleted"},
        {"timestamp": 2, "message": node_finished("model.pkg.a")},
    ]
    with patch.object(fetcher, "_get_log_events", return_value=iter(events)):
        fetcher.collect()
    assert fetcher.command_completed is False
    assert fetcher.events.qsize() == 2


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
def test_fetcher_drains_after_stop_until_command_completed(mock_sleep):
    """After stop(), the events written since the last poll are still read (the provider's loop drops them)."""
    fetcher = make_fetcher()
    fetcher.stop()
    tail = [
        iter([]),  # first drain poll: CloudWatch has not ingested the tail yet
        iter(
            [
                {"timestamp": 1, "message": node_finished("model.pkg.last")},
                {"timestamp": 2, "message": COMMAND_COMPLETED},
            ]
        ),
    ]
    with patch.object(fetcher, "_get_log_events", side_effect=tail) as mock_get:
        fetcher.run()

    assert mock_get.call_count == 2
    assert fetcher.command_completed is True
    assert fetcher.events.qsize() == 2


@patch("cosmos.operators.watcher_aws_ecs.time.monotonic", side_effect=[0.0, 0.0, 10.0])
@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
def test_fetcher_drain_gives_up_after_the_timeout(mock_sleep, mock_monotonic):
    fetcher = make_fetcher(drain_timeout=timedelta(seconds=5))
    fetcher.stop()
    with patch.object(fetcher, "_get_log_events", return_value=iter([])):
        fetcher.run()

    assert fetcher.command_completed is False
    fetcher.logger.warning.assert_called_once()
    assert "CommandCompleted" in fetcher.logger.warning.call_args[0][1]


# ---------------------------------------------------------------------------
# producer execution
# ---------------------------------------------------------------------------


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_process_log_events_parses_the_queue_with_the_execution_context(mock_store):
    op = make_producer(tests_per_model={"model.pkg.a": ["test.pkg.t"]})
    context = {"ti": MagicMock()}
    op._context_holder[CONTEXT_KEY] = context
    op._log_events.put(node_finished("model.pkg.a"))
    op._log_events.put(node_finished("model.pkg.b"))

    op._process_log_events()

    assert mock_store.call_count == 2
    first = mock_store.call_args_list[0]
    assert first.args == (node_finished("model.pkg.a"), {"context": context})
    assert first.kwargs["tests_per_model"] is op._tests_per_model
    assert first.kwargs["test_results_per_model"] is op._test_results_per_model
    assert first.kwargs["model_outlet_uris"] is op._model_outlet_uris
    assert first.kwargs["upstream_failure_skipped_ids"] is op._upstream_failure_skipped_ids
    assert op._log_events.empty()


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_wait_for_task_ended_parses_between_status_polls(mock_client, mock_sleep):
    op = make_producer(waiter_delay=7, waiter_max_attempts=5)
    op.arn = "arn:aws:ecs:eu-west-1:123456789012:task/dbt-cluster/abcdef"
    mock_client.describe_tasks.side_effect = [
        {"tasks": [{"lastStatus": "RUNNING"}]},
        {"tasks": [{"lastStatus": "STOPPED"}]},
    ]
    with patch.object(op, "_process_log_events") as mock_process:
        op._wait_for_task_ended()

    assert mock_process.call_count == 2
    mock_client.describe_tasks.assert_called_with(cluster="dbt-cluster", tasks=[op.arn])
    mock_sleep.assert_called_once_with(7)


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_wait_for_task_ended_fails_when_the_task_never_stops(mock_client, mock_sleep):
    op = make_producer(waiter_delay=1, waiter_max_attempts=2)
    op.arn = "arn:aws:ecs:eu-west-1:123456789012:task/dbt-cluster/abcdef"
    mock_client.describe_tasks.return_value = {"tasks": [{"lastStatus": "RUNNING"}]}
    with patch.object(op, "_process_log_events"), pytest.raises(AirflowException, match="did not stop within 2 polls"):
        op._wait_for_task_ended()


@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_wait_for_task_ended_fails_when_the_task_is_missing(mock_client):
    op = make_producer()
    op.arn = "arn:aws:ecs:eu-west-1:123456789012:task/dbt-cluster/abcdef"
    mock_client.describe_tasks.return_value = {"tasks": [], "failures": [{"reason": "MISSING"}]}
    with patch.object(op, "_process_log_events"), pytest.raises(AirflowException, match="was not found"):
        op._wait_for_task_ended()


@patch("cosmos.operators.aws_ecs.EcsRunTaskOperator._after_execution")
def test_after_execution_parses_the_drained_tail_before_checking_success(mock_parent_after):
    op = make_producer()
    order = []
    mock_parent_after.side_effect = lambda: order.append("check_success")
    with patch.object(op, "_process_log_events", side_effect=lambda: order.append("process")):
        op._after_execution()
    assert order == ["process", "check_success"]


@patch("cosmos.operators._watcher.producer._restore_xcom_from_variable")
@patch("cosmos.operators.aws_ecs.DbtAwsEcsBaseOperator.build_and_run_cmd")
def test_producer_skips_retry_attempt(mock_build_and_run, mock_restore):
    op = make_producer()
    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti, "run_id": "test_run"}
    with pytest.raises(AirflowSkipException, match="does not support Airflow retries"):
        op.execute(context=context)
    mock_restore.assert_called_once_with(context)
    mock_build_and_run.assert_not_called()


@patch("cosmos.operators._watcher.producer._delete_xcom_backup_variable")
@patch("cosmos.operators._watcher.producer._init_xcom_backup")
@patch("cosmos.operators.aws_ecs.DbtAwsEcsBaseOperator.build_and_run_cmd")
def test_producer_first_attempt_exposes_context_and_runs_the_build(mock_build_and_run, mock_init, mock_delete):
    op = make_producer()
    ti = MagicMock()
    ti.try_number = 1
    context = {"ti": ti, "run_id": "test_run"}

    op.execute(context=context)

    assert op._context_holder[CONTEXT_KEY] is context
    mock_init.assert_called_once()
    mock_build_and_run.assert_called_once()
    ti.xcom_push.assert_has_calls([call(key="producer_cmd_flags", value=op.add_cmd_flags())])


# ---------------------------------------------------------------------------
# consumers
# ---------------------------------------------------------------------------


def make_sensor(sensor_class=DbtConsumerWatcherAwsEcsSensor, **kwargs):
    kwargs["extra_context"] = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
    sensor = sensor_class(
        task_id="model.my_model",
        cluster="dbt-cluster",
        task_definition="dbt-task:3",
        project_dir="/tmp/project",
        profile_config=None,
        deferrable=False,
        **kwargs,
    )
    sensor._get_producer_task_status = MagicMock(return_value=None)
    return sensor


def make_context(ti_mock, *, run_id: str = "test-run"):
    return {"ti": ti_mock, "run_id": run_id, "task_instance": MagicMock(map_index=0)}


@patch("cosmos.operators._watcher.base.BaseConsumerSensor._log_startup_events")
def test_consumer_first_execution_polls_the_producer_xcom(mock_startup_events):
    sensor = make_sensor()
    ti = MagicMock()
    ti.try_number = 1
    ti.xcom_pull.return_value = {"status": "success", "outlet_uris": []}
    assert sensor.poke(make_context(ti)) is True
    ti.xcom_pull.assert_called()


@patch("cosmos.operators.aws_ecs.DbtAwsEcsBaseOperator.build_and_run_cmd")
def test_consumer_retry_runs_the_model_in_an_ecs_task(mock_build_and_run_cmd):
    sensor = make_sensor()
    sensor._get_producer_task_status.return_value = "success"
    ti = MagicMock()
    ti.try_number = 2
    ti.xcom_pull.return_value = None
    ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--threads", "2"]
    assert sensor.poke(make_context(ti)) is True
    mock_build_and_run_cmd.assert_called_once()
    assert mock_build_and_run_cmd.call_args.kwargs["cmd_flags"][-2:] == ["--select", "stg_orders"]


@patch("cosmos.operators.aws_ecs.DbtAwsEcsBaseOperator.build_and_run_cmd")
def test_test_sensor_retry_runs_dbt_test_for_the_model(mock_build_and_run_cmd):
    sensor = make_sensor(DbtTestWatcherAwsEcsOperator)
    assert sensor.is_test_sensor is True
    assert sensor.base_cmd == ["test"]
    context = make_context(MagicMock())
    assert sensor._fallback_to_non_watcher_run(try_number=2, context=context) is True
    mock_build_and_run_cmd.assert_called_once_with(context, cmd_flags=["--select", "stg_orders"])


def test_dbt_build_watcher_aws_ecs_operator_raises_not_implemented_error():
    with pytest.raises(NotImplementedError, match="does not expose a DbtBuild operator"):
        DbtBuildWatcherAwsEcsOperator()
