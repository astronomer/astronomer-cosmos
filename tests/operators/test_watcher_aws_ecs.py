import copy
import inspect
import json
from datetime import timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon import __version__ as amazon_provider_version
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from botocore.exceptions import WaiterError
from packaging.version import Version

from cosmos.airflow.graph import _WATCHER_TO_FALLBACK_EXECUTION_MODE, calculate_operator_class
from cosmos.constants import (
    _ECS_WATCHER_MIN_AMAZON_PROVIDER_VERSION,
    PRODUCER_WATCHER_TASK_ID,
    ExecutionMode,
)
from cosmos.exceptions import CosmosValueError
from cosmos.operators._watcher.state import get_status_xcom_key

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
        DbtRunWatcherAwsEcsOperator,
        DbtSeedWatcherAwsEcsOperator,
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


def dbt_event(name: str) -> str:
    return json.dumps({"data": {}, "info": {"name": name, "level": "info", "msg": ""}})


# dbt-core 1.x's last event of a run at the default log level.
STATS_LINE = dbt_event("StatsLine")


TASK_ARN = "arn:aws:ecs:eu-west-1:123456789012:task/dbt-cluster/abcdef"


def make_producer(**overrides):
    return DbtProducerWatcherAwsEcsOperator(**{**ECS_KWARGS, **overrides})


def attach_fetcher(op):
    """Bind the log fetcher the provider's execute() would create, without starting its thread."""
    op.arn = TASK_ARN
    op.task_log_fetcher = op._get_task_log_fetcher()
    return op.task_log_fetcher


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


def test_producer_passes_deferrable_false_to_the_provider():
    """The provider defaults deferrable to [operators] default_deferrable, which the producer cannot honour."""
    with patch.object(EcsRunTaskOperator, "__init__", autospec=True, side_effect=EcsRunTaskOperator.__init__) as init:
        op = make_producer()
    assert init.call_args.kwargs["deferrable"] is False
    assert op.deferrable is False


def test_producer_can_be_deep_copied():
    """Airflow deep-copies operators (task clear, partial_subset); the per-run queue lives on the fetcher."""
    op = make_producer()
    clone = copy.deepcopy(op)
    assert clone.task_id == op.task_id
    assert clone.task_log_fetcher is None


def test_producer_stores_tests_per_model():
    tests_per_model = {"model.pkg.orders": ["test.pkg.t1", "test.pkg.t2"]}
    op = make_producer(tests_per_model=tests_per_model)
    assert op._tests_per_model is tests_per_model
    assert op._test_results_per_model == {}


def test_producer_log_fetcher_is_bound_to_the_operator_state():
    op = make_producer()
    op.arn = TASK_ARN
    fetcher = op._get_task_log_fetcher()
    assert isinstance(fetcher, WatcherEcsLogFetcher)
    assert fetcher.events.empty()
    assert fetcher.events is not op._get_task_log_fetcher().events
    assert fetcher.drain_timeout == op.drain_timeout
    assert fetcher.log_group == "/ecs/dbt"
    assert fetcher.log_stream_name == "ecs/dbt/abcdef"
    assert fetcher.fetch_interval == timedelta(seconds=5)


def test_producer_log_fetcher_reads_the_awslogs_region():
    op = make_producer(awslogs_region="us-east-1")
    op.arn = TASK_ARN
    assert op._get_task_log_fetcher().hook.region_name == "us-east-1"


@pytest.mark.skipif(
    not hasattr(EcsRunTaskOperator, "resolve_awslogs_region"),
    reason="amazon provider releases before 9.34.0 default awslogs_region to the operator's region on init",
)
def test_producer_log_fetcher_falls_back_to_the_operator_region():
    op = make_producer(region_name="eu-west-1")
    op.arn = TASK_ARN
    assert op.awslogs_region is None
    assert op._get_task_log_fetcher().hook.region_name == "eu-west-1"


# ---------------------------------------------------------------------------
# log fetcher
# ---------------------------------------------------------------------------


def make_fetcher(**overrides):
    kwargs = {
        "drain_timeout": timedelta(seconds=3),
        "log_group": "/ecs/dbt",
        "log_stream_name": "ecs/dbt/abcdef",
        "fetch_interval": timedelta(seconds=0),
        "logger": MagicMock(),
        "aws_conn_id": None,
    }
    kwargs.update(overrides)
    return WatcherEcsLogFetcher(**kwargs)


def test_fetcher_collect_queues_messages_and_detects_the_end_of_run():
    fetcher = make_fetcher()
    events = [
        {"timestamp": 1_700_000_000_000, "message": node_finished("model.pkg.a")},
        {"timestamp": 1_700_000_001_000, "message": STATS_LINE},
    ]
    with patch.object(fetcher, "_get_log_events", return_value=iter(events)) as mock_get:
        fetcher.collect()

    mock_get.assert_called_once_with(fetcher._continuation_token)
    assert fetcher.events.get_nowait() == node_finished("model.pkg.a")
    assert fetcher.events.get_nowait() == STATS_LINE
    assert fetcher.read_any_event is True
    assert fetcher.run_completed is True
    assert fetcher.logger.info.call_count == 2


@pytest.mark.parametrize(
    "name, is_end_of_run",
    [
        ("StatsLine", True),  # dbt-core 1.x
        ("NothingToDo", True),  # dbt-core 1.x, empty selection
        ("CommandCompleted", True),  # dbt 2, and dbt-core 1.x at debug level
        ("EndOfRunSummary", False),  # dbt-core 1.x prints StatsLine after it
        ("LogModelResult", False),
    ],
)
def test_fetcher_detects_the_last_event_of_each_dbt_version(name, is_end_of_run):
    fetcher = make_fetcher()
    with patch.object(fetcher, "_get_log_events", return_value=iter([{"timestamp": 1, "message": dbt_event(name)}])):
        fetcher.collect()
    assert fetcher.run_completed is is_end_of_run


def test_fetcher_ignores_non_json_and_other_events_for_completion():
    fetcher = make_fetcher()
    events = [
        {"timestamp": 1, "message": "plain text mentioning StatsLine"},
        {"timestamp": 2, "message": node_finished("model.pkg.a")},
    ]
    with patch.object(fetcher, "_get_log_events", return_value=iter(events)):
        fetcher.collect()
    assert fetcher.run_completed is False
    assert fetcher.events.qsize() == 2


@pytest.mark.parametrize(
    "message",
    [
        '{"info": null, "msg": "StatsLine"}',
        '{"info": "StatsLine"}',
        '["StatsLine"]',
    ],
)
def test_fetcher_ignores_completion_lookalikes_that_are_not_dbt_events(message):
    fetcher = make_fetcher()
    with patch.object(fetcher, "_get_log_events", return_value=iter([{"timestamp": 1, "message": message}])):
        fetcher.collect()
    assert fetcher.run_completed is False


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
def test_fetcher_keeps_polling_after_a_read_error(mock_sleep):
    """An exception escaping run() would end the thread and every later status would be lost."""
    fetcher = make_fetcher()
    reads = [
        RuntimeError("Read timeout on endpoint URL"),
        iter([{"timestamp": 1, "message": STATS_LINE}]),
    ]

    def read(token):
        result = reads.pop(0)
        if isinstance(result, Exception):
            raise result
        fetcher.stop()
        return result

    with patch.object(fetcher, "_get_log_events", side_effect=read):
        fetcher.run()

    assert fetcher.run_completed is True
    fetcher.logger.exception.assert_called_once()


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
def test_fetcher_drains_after_stop_until_the_end_of_run(mock_sleep):
    """After stop(), the events CloudWatch has not served yet are still read."""
    fetcher = make_fetcher()
    fetcher.stop()
    tail = [
        iter([]),  # first drain poll: CloudWatch has not ingested the tail yet
        iter(
            [
                {"timestamp": 1, "message": node_finished("model.pkg.last")},
                {"timestamp": 2, "message": STATS_LINE},
            ]
        ),
    ]
    with patch.object(fetcher, "_get_log_events", side_effect=tail) as mock_get:
        fetcher.run()

    assert mock_get.call_count == 2
    assert fetcher.run_completed is True
    assert fetcher.events.qsize() == 2


@patch("cosmos.operators.watcher_aws_ecs.time.monotonic", side_effect=[0.0, 0.0, 10.0])
@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
def test_fetcher_drain_gives_up_after_the_timeout(mock_sleep, mock_monotonic):
    fetcher = make_fetcher(drain_timeout=timedelta(seconds=5))
    fetcher.stop()
    with patch.object(fetcher, "_get_log_events", return_value=iter([])):
        fetcher.run()

    assert fetcher.run_completed is False
    fetcher.logger.warning.assert_called_once()
    assert "last event of the run was not read" in fetcher.logger.warning.call_args[0][0]


# ---------------------------------------------------------------------------
# producer execution
# ---------------------------------------------------------------------------


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_process_log_events_parses_the_queue_with_the_execution_context(mock_store):
    op = make_producer(tests_per_model={"model.pkg.a": ["test.pkg.t"]})
    fetcher = attach_fetcher(op)
    context = {"ti": MagicMock()}
    op._context_holder[CONTEXT_KEY] = context
    fetcher.events.put(node_finished("model.pkg.a"))
    fetcher.events.put(node_finished("model.pkg.b"))

    op._process_log_events()

    assert mock_store.call_count == 2
    first = mock_store.call_args_list[0]
    assert first.args == (node_finished("model.pkg.a"), {"context": context})
    assert first.kwargs["tests_per_model"] is op._tests_per_model
    assert first.kwargs["test_results_per_model"] is op._test_results_per_model
    assert first.kwargs["model_outlet_uris"] is op._model_outlet_uris
    assert first.kwargs["upstream_failure_skipped_ids"] is op._upstream_failure_skipped_ids
    assert fetcher.events.empty()


@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
def test_process_log_events_without_a_fetcher_is_a_no_op(mock_store):
    make_producer()._process_log_events()
    mock_store.assert_not_called()


@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_wait_for_task_ended_parses_between_status_polls(mock_client, mock_sleep):
    op = make_producer(waiter_delay=7, waiter_max_attempts=5)
    op.arn = TASK_ARN
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
    op.arn = TASK_ARN
    mock_client.describe_tasks.return_value = {"tasks": [{"lastStatus": "RUNNING"}]}
    with patch.object(op, "_process_log_events"), pytest.raises(WaiterError, match="did not stop within 2 polls"):
        op._wait_for_task_ended()


@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_wait_for_task_ended_fails_when_the_task_is_missing(mock_client):
    op = make_producer()
    op.arn = TASK_ARN
    mock_client.describe_tasks.return_value = {"tasks": [], "failures": [{"reason": "MISSING"}]}
    with patch.object(op, "_process_log_events"), pytest.raises(WaiterError, match="was not found"):
        op._wait_for_task_ended()


def start_task_on(op):
    def start(*args):  # amazon provider 8.x passes the context
        op.arn = TASK_ARN

    return patch.object(op, "_start_task", side_effect=start)


@pytest.mark.skipif(
    "stop_task_on_failure" not in inspect.signature(EcsRunTaskOperator.__init__).parameters,
    reason="stop_task_on_failure was added in amazon provider 9.22.0",
)
@patch("cosmos.operators._watcher.producer._init_xcom_backup")
@patch("cosmos.operators.watcher_aws_ecs.WatcherEcsLogFetcher.start")
@patch("cosmos.operators.watcher_aws_ecs.time.sleep")
@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_producer_stops_the_ecs_task_when_waiting_fails(mock_client, mock_sleep, mock_start, mock_init):
    """The provider's stop_task_on_failure cleanup only handles WaiterError, the error its waiter raises."""
    op = make_producer(waiter_delay=0, waiter_max_attempts=1)
    mock_client.describe_tasks.return_value = {"tasks": [{"lastStatus": "RUNNING"}]}
    ti = MagicMock(try_number=1)
    with start_task_on(op), pytest.raises(WaiterError):
        op.execute(context={"ti": ti, "run_id": "test_run"})
    mock_client.stop_task.assert_called_once()
    assert mock_client.stop_task.call_args.kwargs["task"] == TASK_ARN


@patch("cosmos.operators._watcher.producer._init_xcom_backup")
@patch("cosmos.operators.watcher_aws_ecs.WatcherEcsLogFetcher.start")
@patch("cosmos.operators.watcher_aws_ecs.store_dbt_resource_status_from_log")
@patch("cosmos.operators.watcher_aws_ecs.DbtProducerWatcherAwsEcsOperator.client", new_callable=lambda: MagicMock())
def test_producer_publishes_the_statuses_read_before_the_wait_failed(mock_client, mock_store, mock_start, mock_init):
    """The provider skips _after_execution when the wait raises; statuses already queued are still pushed."""
    op = make_producer()

    def wait():
        op.task_log_fetcher.events.put(node_finished("model.pkg.a"))
        raise WaiterError("TasksStopped", "did not stop", {})

    ti = MagicMock(try_number=1)
    with start_task_on(op), patch.object(op, "_wait_for_task_ended", side_effect=wait), pytest.raises(WaiterError):
        op.execute(context={"ti": ti, "run_id": "test_run"})

    mock_store.assert_called_once()
    assert mock_store.call_args.args[0] == node_finished("model.pkg.a")


@patch("cosmos.operators.aws_ecs.EcsRunTaskOperator._after_execution")
def test_after_execution_parses_the_drained_tail_before_checking_success(mock_parent_after):
    op = make_producer()
    order = []
    mock_parent_after.side_effect = lambda: order.append("check_success")
    with patch.object(op, "_process_log_events", side_effect=lambda: order.append("process")):
        op._after_execution()
    assert order == ["process", "check_success"]


@patch("cosmos.operators.aws_ecs.EcsRunTaskOperator._after_execution")
def test_after_execution_forwards_the_provider_arguments(mock_parent_after):
    """Amazon provider 8.3.0 to 8.5.1 call _after_execution(session)."""
    session = MagicMock()
    make_producer()._after_execution(session)
    mock_parent_after.assert_called_once_with(session)


def run_producer(*, log_events, exit_code=0, **overrides):
    """Run the producer through the provider's execute(), with only the ECS and CloudWatch APIs faked.

    The log fetcher thread runs for real; ``log_events`` is what the first CloudWatch read returns.
    """
    op = make_producer(
        container_name="dbt",
        awslogs_fetch_interval=timedelta(0),
        drain_timeout=timedelta(seconds=0.2),
        waiter_delay=0,
        **overrides,
    )
    client = MagicMock()
    client.run_task.return_value = {"tasks": [{"taskArn": TASK_ARN}], "failures": []}
    client.describe_tasks.return_value = {
        "tasks": [
            {"lastStatus": "STOPPED", "containers": [{"name": "dbt", "lastStatus": "STOPPED", "exitCode": exit_code}]}
        ],
        "failures": [],
    }
    reads = [[{"timestamp": 1_700_000_000_000 + i, "message": m} for i, m in enumerate(log_events)]]
    ti = MagicMock(try_number=1)
    with (
        patch.object(DbtProducerWatcherAwsEcsOperator, "client", client),
        patch.object(
            WatcherEcsLogFetcher, "_get_log_events", side_effect=lambda token: iter(reads.pop() if reads else [])
        ),
        patch.object(WatcherEcsLogFetcher, "get_last_log_messages", return_value=[]),
        patch.object(WatcherEcsLogFetcher, "get_last_log_message", return_value=None),
        patch("cosmos.operators.watcher_aws_ecs._DRAIN_POLL_SECONDS", 0.01),
        patch("cosmos.operators._watcher.producer._init_xcom_backup"),
        patch("cosmos.operators._watcher.producer._delete_xcom_backup"),
    ):
        try:
            op.execute(context={"ti": ti, "run_id": "test_run"})
        finally:
            op.task_log_fetcher.join(timeout=5)
            assert not op.task_log_fetcher.is_alive()
    return op, ti


def pushed_statuses(ti):
    return {
        c.kwargs["key"]: c.kwargs["value"]
        for c in ti.xcom_push.call_args_list
        if c.kwargs.get("key", "").endswith("_status")
    }


def test_producer_publishes_statuses_through_the_provider_execute():
    op, ti = run_producer(log_events=[node_finished("model.pkg.a"), STATS_LINE])
    assert pushed_statuses(ti) == {get_status_xcom_key("model.pkg.a"): {"status": "success", "outlet_uris": []}}
    assert op.task_log_fetcher.run_completed is True


def test_producer_fails_when_no_log_event_was_read():
    """A wrong awslogs_stream_prefix or a missing logs:GetLogEvents reads nothing; the build itself succeeded."""
    with pytest.raises(
        AirflowException, match="no event was read from log stream ecs/dbt/abcdef in log group /ecs/dbt"
    ):
        run_producer(log_events=[])


def test_producer_reports_a_failed_container_before_an_empty_log_stream():
    with pytest.raises(AirflowException, match="not in success state"):
        run_producer(log_events=[], exit_code=1)


@patch("cosmos.settings.enable_watcher_reliable_retry", False)
@patch("cosmos.operators._watcher.producer._restore_xcom_backup")
@patch("cosmos.operators.aws_ecs.DbtAwsEcsBaseOperator.build_and_run_cmd")
def test_producer_skips_retry_attempt(mock_build_and_run, mock_restore):
    op = make_producer()
    ti = MagicMock()
    ti.try_number = 2
    context = {"ti": ti, "run_id": "test_run"}
    with pytest.raises(AirflowSkipException, match="does not support Airflow retries"):
        op.execute(context=context)
    mock_restore.assert_called_once_with(context, read_task_state_store=False)
    mock_build_and_run.assert_not_called()


@patch("cosmos.operators._watcher.producer._delete_xcom_backup")
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


@pytest.mark.parametrize(
    "sensor_class, base_cmd", [(DbtSeedWatcherAwsEcsOperator, ["seed"]), (DbtRunWatcherAwsEcsOperator, ["run"])]
)
def test_seed_and_run_consumers_are_created(sensor_class, base_cmd):
    assert make_sensor(sensor_class).base_cmd == base_cmd


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
    mock_build_and_run_cmd.assert_called_once()
    assert mock_build_and_run_cmd.call_args.args == (context,)
    assert mock_build_and_run_cmd.call_args.kwargs["cmd_flags"] == ["--select", "stg_orders"]


@pytest.mark.parametrize("sensor_class", [DbtRunWatcherAwsEcsOperator, DbtTestWatcherAwsEcsOperator])
def test_deferrable_consumer_runs_its_fallback_ecs_task_synchronously(sensor_class):
    """A deferred ECS task would resume in the sensor's execute_complete, which ignores the exit code."""
    sensor = make_sensor(sensor_class)
    sensor.deferrable = True
    seen = []
    with (
        patch.object(
            EcsRunTaskOperator, "execute", autospec=True, side_effect=lambda op, ctx: seen.append(op.deferrable)
        ),
        patch.object(sensor, "build_command"),
    ):
        sensor._fallback_to_non_watcher_run(try_number=2, context=make_context(MagicMock()))

    assert seen == [False]
    assert sensor.deferrable is True


def test_dbt_build_watcher_aws_ecs_operator_raises_not_implemented_error():
    with pytest.raises(NotImplementedError, match="does not expose a DbtBuild operator"):
        DbtBuildWatcherAwsEcsOperator()
