"""
Integration tests for adapter-native metric views (Databricks) and semantic views (Snowflake), PR #2885,
in the style of tests/operators/test_watcher.py::test_dbt_dag_with_watcher: each test builds a DbtDag
against a real warehouse, checks the topology (nodes, task ids, operator classes, edges), executes it,
and verifies the objects in the warehouse. Topology-only checks for the other test behaviours, the
selectors and the setting live in tests/airflow/test_metric_and_semantic_view_dags.py.

One-time setup, from the repository root with the test virtualenv active::

    export AIRFLOW_HOME=/tmp/cosmos-e2e-airflow && airflow db migrate
    pytest tests/e2e/test_metric_and_semantic_views.py -m warehouse_e2e -s -vv

Credentials are described in ``tests/e2e/warehouses.py``. A warehouse without credentials is skipped.
"""

from __future__ import annotations

import warnings
from collections import defaultdict
from datetime import datetime

import pytest
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.operators.base import DbtSemanticMetricViewMixin
from cosmos.operators.local import (
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    DbtSemanticMetricViewLocalOperator,
    DbtTestLocalOperator,
)
from cosmos.operators.watcher import (
    DbtProducerWatcherOperator,
    DbtRunWatcherOperator,
    DbtSeedWatcherOperator,
    DbtSemanticMetricViewWatcherOperator,
    DbtTestWatcherOperator,
)

from .. import utils as test_utils
from .warehouses import (
    RECLASSIFIED_RESOURCE_TYPE,
    RECLASSIFIED_TASK_SUFFIX,
    SEEDED_REVENUE_BY_STATUS,
    DatabricksHarness,
    SnowflakeHarness,
    WarehouseHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.warehouse_e2e]

EXPECTED_NODE_COUNT = 9


@pytest.fixture(scope="session", params=[SnowflakeHarness, DatabricksHarness], ids=["snowflake", "databricks"])
def warehouse(request) -> WarehouseHarness:
    harness = request.param()
    harness.configure()
    yield harness
    try:
        with harness.connect() as connection, connection.cursor() as cursor:
            harness.drop_schema(cursor)
    except Exception as error:
        warnings.warn(f"Could not drop {harness.name} schema {harness.schema}: {error}")


def build_dag(
    warehouse: WarehouseHarness,
    dag_id: str,
    execution_mode: ExecutionMode = ExecutionMode.LOCAL,
    test_behavior: TestBehavior = TestBehavior.AFTER_EACH,
) -> DbtDag:
    return DbtDag(
        project_config=ProjectConfig(dbt_project_path=warehouse.project_dir),
        profile_config=warehouse.profile_config(),
        render_config=RenderConfig(test_behavior=test_behavior),
        execution_config=ExecutionConfig(execution_mode=execution_mode),
        operator_args={"install_deps": True, "full_refresh": True},
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"retries": 0},
    )


def topological_task_ids(dag: DbtDag) -> list[str]:
    return [task.task_id for task in dag.topological_sort()]


def flat_chain(warehouse: WarehouseHarness) -> list[str]:
    semantic = warehouse.semantic_object_model
    return ["raw_orders_seed", "orders_run", f"{semantic}_{RECLASSIFIED_TASK_SUFFIX}", "revenue_by_status_run"]


def grouped_chain(warehouse: WarehouseHarness) -> list[str]:
    semantic = warehouse.semantic_object_model
    return [
        "raw_orders_seed",
        "orders.run",
        "orders.test",
        f"{semantic}.{RECLASSIFIED_TASK_SUFFIX}",
        f"{semantic}.test",
        "revenue_by_status.run",
        "revenue_by_status.test",
    ]


def make_watcher_consumers_wait_for_producer(dag: DbtDag) -> None:
    # dag.test() ignores priority_weight, so a consumer sensor scheduled before the producer
    # waits forever for XComs the producer can never push in a single-process run.
    for task in dag.tasks:
        if task.task_id != "dbt_producer_watcher" and task.trigger_rule == TriggerRule.ALWAYS:
            task.trigger_rule = TriggerRule.ALL_SUCCESS


def run_dag_and_collect_states(dag: DbtDag) -> tuple[DagRunState, dict[str, str]]:
    if test_utils.AIRFLOW_VERSION.release[:2] >= (3, 1):
        test_utils.serialize_dag_to_db(dag)
        dag_run = dag.test(logical_date=test_utils.timezone.utcnow())
    else:
        dag_run = dag.test()
    with create_session() as session:
        states = {ti.task_id: ti.state for ti in dag_run.get_task_instances(session=session)}
    return dag_run.state, states


def assert_dag_succeeded(dag: DbtDag) -> dict[str, str]:
    dag_run_state, states = run_dag_and_collect_states(dag)
    failed = {task_id: state for task_id, state in states.items() if state != TaskInstanceState.SUCCESS}
    assert dag_run_state == DagRunState.SUCCESS and not failed, f"{dag.dag_id}: {failed}"
    return states


def assert_warehouse_objects(warehouse: WarehouseHarness) -> None:
    with warehouse.connect() as connection, connection.cursor() as cursor:
        description = warehouse.describe_object(cursor, warehouse.semantic_object_model)
        assert warehouse.is_semantic_object(description), description
        assert warehouse.fetch_revenue_by_status_from_semantic_object(cursor) == SEEDED_REVENUE_BY_STATUS
        assert warehouse.fetch_downstream_revenue_by_status(cursor) == SEEDED_REVENUE_BY_STATUS


def asset_event_uris_by_task(dag_id: str) -> dict[str, list[str]]:
    try:
        from airflow.models.asset import AssetEvent
    except ImportError:
        return {}
    uris: dict[str, list[str]] = defaultdict(list)
    with create_session() as session:
        for event in session.query(AssetEvent).filter(AssetEvent.source_dag_id == dag_id).all():
            uris[event.source_task_id].append(event.asset.uri)
    return dict(uris)


def test_dbt_dag_with_local_execution(warehouse: WarehouseHarness):
    dag = build_dag(warehouse, dag_id=warehouse.dag_id("local"))
    semantic = warehouse.semantic_object_model

    assert len(dag.dbt_graph.filtered_nodes) == EXPECTED_NODE_COUNT
    assert len(dag.task_dict) == 7
    assert topological_task_ids(dag) == grouped_chain(warehouse)
    assert isinstance(dag.task_dict["raw_orders_seed"], DbtSeedLocalOperator)
    assert isinstance(dag.task_dict["orders.run"], DbtRunLocalOperator)
    assert isinstance(dag.task_dict["revenue_by_status.run"], DbtRunLocalOperator)
    assert isinstance(dag.task_dict["orders.test"], DbtTestLocalOperator)
    assert isinstance(dag.task_dict[f"{semantic}.test"], DbtTestLocalOperator)
    semantic_task = dag.task_dict[f"{semantic}.{RECLASSIFIED_TASK_SUFFIX}"]
    assert isinstance(semantic_task, DbtSemanticMetricViewLocalOperator)
    assert semantic_task.ui_color == DbtSemanticMetricViewMixin.ui_color
    assert semantic_task.base_cmd == ["run"]
    assert semantic_task.select == f"fqn:{warehouse.project_name}.{semantic}"
    assert semantic_task.upstream_task_ids == {"orders.test"}
    assert semantic_task.downstream_task_ids == {f"{semantic}.test"}
    assert dag.task_dict[f"{semantic}.test"].downstream_task_ids == {"revenue_by_status.run"}

    assert_dag_succeeded(dag)
    assert_warehouse_objects(warehouse)

    assert_dag_succeeded(dag)
    assert_warehouse_objects(warehouse)

    events = asset_event_uris_by_task(dag.dag_id)
    if not events.get("orders.run"):
        pytest.skip("asset events are not observable through dag.test() on this Airflow version")
    assert events.get(
        f"{semantic}.{RECLASSIFIED_TASK_SUFFIX}"
    ), f"the reclassified task stopped emitting asset events; events recorded: {events}"


@pytest.mark.parametrize("test_behavior", [TestBehavior.AFTER_EACH, TestBehavior.NONE], ids=["after_each", "none"])
def test_dbt_dag_with_watcher_execution(warehouse: WarehouseHarness, test_behavior: TestBehavior):
    dag = build_dag(
        warehouse,
        dag_id=warehouse.dag_id(f"watcher_{test_behavior.value}"),
        execution_mode=ExecutionMode.WATCHER,
        test_behavior=test_behavior,
    )
    semantic = warehouse.semantic_object_model
    grouped = test_behavior == TestBehavior.AFTER_EACH
    semantic_task_id = f"{semantic}.{RECLASSIFIED_TASK_SUFFIX}" if grouped else f"{semantic}_{RECLASSIFIED_TASK_SUFFIX}"
    seed_task_id = "raw_orders_seed"
    orders_task_id = "orders.run" if grouped else "orders_run"

    assert len(dag.dbt_graph.filtered_nodes) == EXPECTED_NODE_COUNT
    expected_chain = grouped_chain(warehouse) if grouped else flat_chain(warehouse)
    assert topological_task_ids(dag) == ["dbt_producer_watcher", *expected_chain]
    producer = dag.task_dict["dbt_producer_watcher"]
    assert isinstance(producer, DbtProducerWatcherOperator)
    assert isinstance(dag.task_dict[seed_task_id], DbtSeedWatcherOperator)
    assert isinstance(dag.task_dict[orders_task_id], DbtRunWatcherOperator)
    assert isinstance(dag.task_dict[semantic_task_id], DbtSemanticMetricViewWatcherOperator)
    if grouped:
        assert isinstance(dag.task_dict["orders.test"], DbtTestWatcherOperator)
    assert "dbt_producer_watcher" in dag.task_dict[seed_task_id].upstream_task_ids

    command, _ = producer.build_cmd({}, {})
    flags = [str(part) for part in command]
    resource_types = {flags[index + 1] for index, part in enumerate(flags) if part == "--resource-type"}
    assert resource_types == (set() if grouped else {"model", "snapshot", "seed"})
    assert RECLASSIFIED_RESOURCE_TYPE not in flags

    make_watcher_consumers_wait_for_producer(dag)
    assert_dag_succeeded(dag)
    assert_warehouse_objects(warehouse)
