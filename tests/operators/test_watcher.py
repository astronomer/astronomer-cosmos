import base64
import json
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.utils.state import DagRunState
from packaging.version import Version

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.config import InvocationMode
from cosmos.constants import ExecutionMode
from cosmos.operators.watcher import (
    PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT,
    DbtBuildWatcherOperator,
    DbtConsumerWatcherSensor,
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtTestWatcherOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from tests.utils import AIRFLOW_VERSION

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"
DBT_PROFILES_YAML_FILEPATH = DBT_PROJECT_PATH / "profiles.yml"


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

    def xcom_push(self, key: str, value: str, **_):
        self.store[key] = value


class _MockContext(dict):
    pass


def _fake_event(name: str = "NodeFinished", uid: str = "model.pkg.m"):
    """Create a minimal fake EventMsg-like object suitable for helper tests."""

    class _Info(SimpleNamespace):
        pass

    class _NodeInfo(SimpleNamespace):
        pass

    class _RunResult(SimpleNamespace):
        pass

    node_info = _NodeInfo(unique_id=uid)
    run_result = _RunResult(status="success", message="ok")

    data = SimpleNamespace(node_info=node_info, run_result=run_result)
    info = _Info(name=name, code="X", msg="msg")
    return SimpleNamespace(info=info, data=data)


@patch("google.protobuf.json_format.MessageToDict")
def test_serialize_event(mock_mtd):
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)

    mock_mtd.side_effect = lambda ev, **kwargs: {"dummy": True}

    out = op._serialize_event(_fake_event())
    assert out == {"dummy": True}
    mock_mtd.assert_called()


def test_dbt_producer_watcher_operator_priority_weight_default():
    """Test that DbtProducerWatcherOperator uses default priority_weight of 9999."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.priority_weight == PRODUCER_OPERATOR_DEFAULT_PRIORITY_WEIGHT


def test_dbt_producer_watcher_operator_priority_weight_override():
    """Test that DbtProducerWatcherOperator allows overriding priority_weight."""
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None, priority_weight=100)
    assert op.priority_weight == 100


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


def test_handle_startup_event():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    lst: list[dict] = []
    ev = _fake_event("MainReportVersion")
    op._handle_startup_event(ev, lst)
    assert lst and lst[0]["name"] == "MainReportVersion"


def test_handle_node_finished_pushes_xcom():
    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    ti = _MockTI()
    ctx = _MockContext(ti=ti)

    with patch.object(op, "_serialize_event", return_value={"foo": "bar"}):
        ev = _fake_event()
        op._handle_node_finished(ev, ctx)

    stored = list(ti.store.values())[0]
    raw = zlib.decompress(base64.b64decode(stored)).decode()
    assert json.loads(raw) == {"foo": "bar"}


def test_execute_streaming_mode():
    """Streaming path should push startup + per-model XComs."""
    from contextlib import nullcontext

    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    op.invocation_mode = InvocationMode.DBT_RUNNER

    import cosmos.operators.watcher as _watch_mod

    # Ensure EventMsg symbol exists without permanently altering the module
    if _watch_mod.EventMsg is None:

        class _DummyEv:
            pass

        eventmsg_patch = patch("cosmos.operators.watcher.EventMsg", _DummyEv, create=True)
    else:
        eventmsg_patch = nullcontext()  # type: ignore

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "dummy"}

    main_rep = _fake_event("MainReportVersion")
    node_evt = _fake_event("NodeFinished", uid="model.pkg.x")

    def fake_base_execute(self, context=None, **_):  # type: ignore[override]
        for cb in getattr(self, "_dbt_runner_callbacks", []):
            cb(main_rep)
            cb(node_evt)
        return None

    with eventmsg_patch, patch.object(
        DbtProducerWatcherOperator,
        "_serialize_event",
        lambda self, ev: {"dummy": True},
    ), patch(
        "cosmos.operators.watcher.DbtLocalBaseOperator.execute",
        fake_base_execute,
    ):
        op.execute(context=ctx)

    assert "dbt_startup_events" in ti.store

    node_key = "nodefinished_model__pkg__x"
    assert node_key in ti.store


def test_execute_fallback_mode(tmp_path):
    """Fallback path pushes compressed run_results once."""

    tgt = tmp_path / "target"
    tgt.mkdir()
    with (tgt / "run_results.json").open("w") as fp:
        json.dump({"results": [{"unique_id": "a", "status": "success"}]}, fp)

    op = DbtProducerWatcherOperator(project_dir=str(tmp_path), profile_config=None)
    op.invocation_mode = InvocationMode.SUBPROCESS  # force fallback

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "x"}

    def fake_build_run(self, context, **kw):
        from cosmos.operators.local import AbstractDbtLocalBase

        AbstractDbtLocalBase._handle_post_execution(self, self.project_dir, context, True)
        return None

    with patch("cosmos.operators.local.DbtLocalBaseOperator.build_and_run_cmd", fake_build_run):
        op.execute(context=ctx)

    compressed = ti.store.get("run_results")
    assert compressed
    data = json.loads(zlib.decompress(base64.b64decode(compressed)).decode())
    assert data["results"][0]["status"] == "success"


@patch("cosmos.dbt.runner.is_available", return_value=False)
@patch("cosmos.operators.watcher.DbtLocalBaseOperator.execute", return_value="done")
def test_execute_discovers_invocation_mode(_mock_execute, _mock_is_available):
    """If invocation_mode is unset, execute() should discover and set it."""

    from cosmos.config import InvocationMode

    op = DbtProducerWatcherOperator(project_dir=".", profile_config=None)
    assert op.invocation_mode is None  # precondition

    ti = _MockTI()
    ctx = {"ti": ti, "run_id": "xyz"}

    result = op.execute(context=ctx)

    assert result == "done"
    assert op.invocation_mode == InvocationMode.SUBPROCESS


MODEL_UNIQUE_ID = "model.jaffle_shop.stg_orders"
ENCODED_RUN_RESULTS = base64.b64encode(
    zlib.compress(b'{"results":[{"unique_id":"model.jaffle_shop.stg_orders","status":"success"}]}')
).decode("utf-8")

ENCODED_RUN_RESULTS_FAILED = base64.b64encode(
    zlib.compress(b'{"results":[{"unique_id":"model.jaffle_shop.stg_orders","status":"fail"}]}')
).decode("utf-8")

ENCODED_EVENT = base64.b64encode(zlib.compress(b'{"data": {"run_result": {"status": "success"}}}')).decode("utf-8")


class TestDbtConsumerWatcherSensor:

    def make_sensor(self, **kwargs):
        extra_context = {"dbt_node_config": {"unique_id": "model.jaffle_shop.stg_orders"}}
        kwargs["extra_context"] = extra_context
        sensor = DbtConsumerWatcherSensor(
            task_id="model.my_model",
            project_dir="/tmp/project",
            profile_config=None,
            **kwargs,
        )

        sensor.invocation_mode = "DBT_RUNNER"
        return sensor

    def make_context(self, ti_mock):
        return {"ti": ti_mock}

    @patch("cosmos.operators.watcher.EventMsg")
    def test_poke_status_none_from_events(self, MockEventMsg):
        mock_event_instance = MagicMock()
        mock_event_instance.status = "done"
        MockEventMsg.return_value = mock_event_instance

        sensor = self.make_sensor()
        sensor.invocation_mode = InvocationMode.DBT_RUNNER
        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.side_effect = [None, None]  # no event msg found
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is False

    def test_poke_success_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "SUBPROCESS"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    def test_invocation_mode_none(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = None

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is True

    def test_poke_failure_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "OTHER_MODE"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS_FAILED
        context = self.make_context(ti)

        with pytest.raises(AirflowException):
            sensor.poke(context)

    def test_poke_status_none_from_run_results(self):
        sensor = self.make_sensor()
        sensor.invocation_mode = "OTHER_MODE"

        ti = MagicMock()
        ti.try_number = 1
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        result = sensor.poke(context)
        assert result is False

    @patch("cosmos.operators.local.AbstractDbtLocalBase.build_and_run_cmd")
    def test_task_retry(self, mock_build_and_run_cmd):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.try_number = 2
        ti.xcom_pull.return_value = None
        context = self.make_context(ti)

        sensor.poke(context)
        mock_build_and_run_cmd.assert_called_once()

    def test_handle_task_retry(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.task.dag.get_task.return_value.add_cmd_flags.return_value = ["--select", "some_model", "--threads", "2"]
        context = self.make_context(ti)
        sensor.build_and_run_cmd = MagicMock()

        result = sensor._handle_task_retry(2, context)

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

    def test_get_status_from_run_results_success(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = ENCODED_RUN_RESULTS

        result = sensor._get_status_from_run_results(ti)
        assert result == "success"

    def test_get_status_from_run_results_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.return_value = None

        result = sensor._get_status_from_run_results(ti)
        assert result is None

    def test_get_status_from_events_success(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, ENCODED_EVENT]

        result = sensor._get_status_from_events(ti)
        assert result == "success"

    def test_get_status_from_events_none(self):
        sensor = self.make_sensor()
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, None]

        result = sensor._get_status_from_events(ti)
        assert result is None


class TestDbtBuildWatcherOperator:

    def test_dbt_build_watcher_operator_raises_not_implemented_error(self):
        expected_message = (
            "`ExecutionMode.WATCHER` does not expose a DbtBuild operator, "
            "since the build command is executed by the producer task."
        )

        with pytest.raises(NotImplementedError, match=expected_message):
            DbtBuildWatcherOperator()


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
@pytest.mark.integration
def test_dbt_dag_with_watcher():
    """
    Run a DbtDag using `ExecutionMode.WATCHER`.
    Confirm the right amount of tasks is created and that tasks are in the expected topological order.
    Confirm that the producer watcher task is created and that it is the parent of the root dbt nodes.
    """
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
    outcome = watcher_dag.test()
    assert outcome.state == DagRunState.SUCCESS

    assert len(watcher_dag.dbt_graph.filtered_nodes) == 26

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


@pytest.mark.skipif(AIRFLOW_VERSION < Version("2.7"), reason="Airflow did not have dag.test() until the 2.6 release")
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

        pre_dbt >> dbt_task_group

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
        "dbt_task_group.raw_customers_seed",
        "dbt_task_group.raw_orders_seed",
        "dbt_task_group.raw_payments_seed",
        "dbt_task_group.dbt_producer_watcher",
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
