"""
Integration tests for the opt-in semantic metric view resource type against real warehouses: Databricks metric
views and Snowflake semantic views. The LOCAL and WATCHER cases load the example DAGs from dev/dags, check their
topology, run them and verify the objects in the warehouse, in the style of
tests/operators/test_watcher.py::test_dbt_dag_with_watcher.

Credentials, as provided by CI: ``AIRFLOW_CONN_DATABRICKS_DEFAULT`` plus ``DATABRICKS_CATALOG`` (default ``main``),
and ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``, ``SNOWFLAKE_PRIVATE_KEY`` (PKCS#8 PEM), ``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_SCHEMA``,
``SNOWFLAKE_WAREHOUSE`` (optional ``SNOWFLAKE_ROLE``). A warehouse with missing credentials, or that cannot be set up,
is skipped locally and fails under CI (``CI`` set).
Databricks gets a throwaway schema. The Snowflake CI role cannot create schemas, so its objects go into the existing
``SNOWFLAKE_SCHEMA`` under a per-run ``RESOURCE_PREFIX`` and only those objects are dropped afterwards.
"""

from __future__ import annotations

import json
import os
import uuid
import warnings
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from airflow.models.connection import Connection
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState

try:
    from airflow.task.trigger_rule import TriggerRule
except ImportError:  # pragma: no cover
    from airflow.utils.trigger_rule import TriggerRule

from cosmos import DbtDag, ProfileConfig, TestBehavior, settings
from cosmos.constants import AIRFLOW_VERSION, DbtResourceType
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
from cosmos.profiles import DatabricksTokenProfileMapping, SnowflakePrivateKeyPemProfileMapping

from . import utils as test_utils

pytestmark = pytest.mark.integration

DEV_DAGS_DIR = Path(__file__).parent.parent / "dev" / "dags"
DBT_ROOT_PATH = DEV_DAGS_DIR / "dbt"
SEEDED_REVENUE_BY_STATUS = {"cancelled": 75.25, "completed": 851.25, "pending": 45.00}
EXPECTED_NODE_COUNT = 9


def skip_unless_ci(reason: str) -> None:
    if os.getenv("CI"):
        pytest.fail(reason)
    pytest.skip(reason)


def private_key_der(private_key_pem: str) -> bytes:
    from cryptography.hazmat.primitives import serialization

    private_key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def amounts_by_status(rows: list[tuple[Any, Any]]) -> dict[str, float]:
    return {str(status).lower(): float(round(Decimal(str(amount)), 2)) for status, amount in rows}


class Warehouse:
    name: str
    project_name: str
    semantic_model: str
    status_column: str
    test_behavior: TestBehavior

    def __init__(self) -> None:
        self.schema = f"cosmos_e2e_{uuid.uuid4().hex[:8]}"
        self.resource_prefix = ""
        os.environ["COSMOS_DEMO_SCHEMA"] = self.schema

    def object_name(self, name: str) -> str:
        return f"{self.resource_prefix}_{name}" if self.resource_prefix else name

    @property
    def project_dir(self) -> Path:
        return DBT_ROOT_PATH / self.project_name

    def configure(self) -> None:
        raise NotImplementedError

    def profile_config(self) -> ProfileConfig:
        raise NotImplementedError

    def connect(self) -> Any:
        raise NotImplementedError

    def driver_error(self) -> type[Exception]:
        raise NotImplementedError

    def qualified(self, name: str) -> str:
        raise NotImplementedError

    def is_semantic_object(self, cursor: Any) -> bool:
        raise NotImplementedError

    def revenue_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        raise NotImplementedError

    def revenue_from_downstream_table(self, cursor: Any) -> dict[str, float]:
        cursor.execute(f"SELECT {self.status_column}, total_revenue FROM {self.qualified('revenue_by_status')}")
        return amounts_by_status(cursor.fetchall())

    def set_up(self, cursor: Any) -> None:
        cursor.execute(f"CREATE SCHEMA {self.qualified_schema()}")

    def tear_down(self, cursor: Any) -> None:
        cursor.execute(f"DROP SCHEMA IF EXISTS {self.qualified_schema()} CASCADE")

    def qualified_schema(self) -> str:
        raise NotImplementedError


class Databricks(Warehouse):
    name = "databricks"
    project_name = "databricks_metric_view"
    semantic_model = "order_metrics"
    status_column = "status"
    test_behavior = TestBehavior.AFTER_ALL

    def configure(self) -> None:
        if not os.getenv("AIRFLOW_CONN_DATABRICKS_DEFAULT"):
            skip_unless_ci("databricks: AIRFLOW_CONN_DATABRICKS_DEFAULT is not set")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "main")
        connection = Connection.get_connection_from_secrets("databricks_default")
        self.connection_params = {
            "server_hostname": connection.host.replace("https://", "").rstrip("/"),
            "http_path": connection.extra_dejson["http_path"],
            "access_token": connection.password or connection.extra_dejson.get("token"),
        }

    def profile_config(self) -> ProfileConfig:
        return ProfileConfig(
            profile_name=self.project_name,
            target_name="dev",
            profile_mapping=DatabricksTokenProfileMapping(
                conn_id="databricks_default", profile_args={"catalog": self.catalog, "schema": self.schema}
            ),
        )

    def connect(self) -> Any:
        from databricks import sql

        return sql.connect(**self.connection_params)

    def driver_error(self) -> type[Exception]:
        from databricks.sql.exc import Error

        return Error

    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def qualified(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def is_semantic_object(self, cursor: Any) -> bool:
        cursor.execute(f"DESCRIBE TABLE EXTENDED {self.qualified(self.semantic_model)}")
        details = {row[0]: str(row[1]) for row in cursor.fetchall() if row[0]}
        return details.get("Type") == "METRIC_VIEW"

    def revenue_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        cursor.execute(
            f"SELECT status, MEASURE(total_revenue) FROM {self.qualified(self.semantic_model)} GROUP BY status"
        )
        return amounts_by_status(cursor.fetchall())


class Snowflake(Warehouse):
    name = "snowflake"
    project_name = "snowflake_semantic_view"
    semantic_model = "orders_semantic_view"
    status_column = "order_status"
    test_behavior = TestBehavior.AFTER_EACH

    def configure(self) -> None:
        required = (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_WAREHOUSE",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            skip_unless_ci(f"snowflake: missing {missing}")
        self.database = os.environ["SNOWFLAKE_DATABASE"]
        self.schema = os.environ["SNOWFLAKE_SCHEMA"]
        self.resource_prefix = f"cosmos_e2e_{uuid.uuid4().hex[:8]}"
        os.environ["COSMOS_DEMO_SCHEMA"] = self.schema
        os.environ["RESOURCE_PREFIX"] = self.resource_prefix
        extra = {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
            "database": self.database,
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "private_key_content": os.environ["SNOWFLAKE_PRIVATE_KEY"],
        }
        os.environ["AIRFLOW_CONN_SNOWFLAKE_DEFAULT"] = json.dumps(
            {
                "conn_type": "snowflake",
                "login": os.environ["SNOWFLAKE_USER"],
                "schema": self.schema,
                "extra": {key: value for key, value in extra.items() if value},
            }
        )
        self.connection_params = {
            "account": extra["account"],
            "user": os.environ["SNOWFLAKE_USER"],
            "private_key": private_key_der(os.environ["SNOWFLAKE_PRIVATE_KEY"]),
            "warehouse": extra["warehouse"],
            "database": self.database,
            **({"role": extra["role"]} if extra["role"] else {}),
        }

    def profile_config(self) -> ProfileConfig:
        return ProfileConfig(
            profile_name=self.project_name,
            target_name="dev",
            profile_mapping=SnowflakePrivateKeyPemProfileMapping(
                conn_id="snowflake_default", profile_args={"schema": self.schema}
            ),
        )

    def connect(self) -> Any:
        import snowflake.connector

        return snowflake.connector.connect(**self.connection_params)

    def driver_error(self) -> type[Exception]:
        from snowflake.connector.errors import Error

        return Error

    def qualified_schema(self) -> str:
        return f"{self.database}.{self.schema}"

    def qualified(self, name: str) -> str:
        return f"{self.database}.{self.schema}.{self.object_name(name)}"

    def set_up(self, cursor: Any) -> None:
        cursor.execute(f"SHOW TERSE TABLES IN SCHEMA {self.qualified_schema()} LIMIT 1")

    def tear_down(self, cursor: Any) -> None:
        cursor.execute(f"DROP SEMANTIC VIEW IF EXISTS {self.qualified(self.semantic_model)}")
        for table in ("revenue_by_status", "orders", "raw_orders"):
            cursor.execute(f"DROP TABLE IF EXISTS {self.qualified(table)}")

    def is_semantic_object(self, cursor: Any) -> bool:
        semantic_view_name = self.object_name(self.semantic_model).upper()
        cursor.execute(f"SHOW SEMANTIC VIEWS LIKE '{semantic_view_name}' IN SCHEMA {self.qualified_schema()}")
        return len(cursor.fetchall()) == 1

    def revenue_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        cursor.execute(
            "SELECT order_status, total_revenue FROM SEMANTIC_VIEW("
            f"{self.qualified(self.semantic_model)} DIMENSIONS orders.order_status METRICS orders.total_revenue)"
        )
        return amounts_by_status(cursor.fetchall())


@pytest.fixture(scope="module", autouse=True)
def _cosmos_settings():
    original = (settings.enable_cache, settings.enable_semantic_metric_view_reclassification)
    settings.enable_cache = False
    settings.enable_semantic_metric_view_reclassification = True
    yield
    settings.enable_cache, settings.enable_semantic_metric_view_reclassification = original


@pytest.fixture(scope="module", params=[Snowflake, Databricks], ids=["snowflake", "databricks"])
def warehouse(request) -> Warehouse:
    instance = request.param()
    instance.configure()
    try:
        with instance.connect() as connection, connection.cursor() as cursor:
            instance.set_up(cursor)
    except instance.driver_error() as error:
        skip_unless_ci(f"{instance.name}: cannot set up the test objects: {error}")
    yield instance
    try:
        with instance.connect() as connection, connection.cursor() as cursor:
            instance.tear_down(cursor)
    except instance.driver_error() as error:
        warnings.warn(f"could not drop the {instance.name} test objects in {instance.schema}: {error}")
    finally:
        os.environ.pop("RESOURCE_PREFIX", None)


def load_example_dag(warehouse: Warehouse, variant: str) -> DbtDag:
    dag_bag = test_utils.make_dag_bag(
        dag_folder=str(DEV_DAGS_DIR / f"example_{warehouse.project_name}.py"), include_examples=False
    )
    assert not dag_bag.import_errors, dag_bag.import_errors
    dag = dag_bag.get_dag(f"{warehouse.project_name}_{variant}")
    assert dag is not None, dag_bag.dag_ids
    return dag


def expected_task_chain(warehouse: Warehouse) -> list[str]:
    semantic = warehouse.semantic_model
    if warehouse.test_behavior == TestBehavior.AFTER_EACH:
        return [
            "raw_orders_seed",
            "orders.run",
            "orders.test",
            f"{semantic}.semantic_metric_run",
            f"{semantic}.test",
            "revenue_by_status.run",
            "revenue_by_status.test",
        ]
    return [
        "raw_orders_seed",
        "orders_run",
        f"{semantic}_semantic_metric_run",
        "revenue_by_status_run",
        f"{warehouse.project_name}_test",
    ]


def topological_task_ids(dag: DbtDag) -> list[str]:
    return [task.task_id for task in dag.topological_sort()]


def make_watcher_consumers_wait_for_producer(dag: DbtDag) -> None:
    # dag.test() ignores priority_weight, so a consumer sensor scheduled before the producer
    # waits forever for XComs the producer can never push in a single-process run.
    for task in dag.tasks:
        if task.task_id != "dbt_producer_watcher" and task.trigger_rule == TriggerRule.ALWAYS:
            task.trigger_rule = TriggerRule.ALL_SUCCESS


def run_dag(dag: DbtDag) -> dict[str, str]:
    dag_run = test_utils.new_test_dag(dag)
    with create_session() as session:
        states = {ti.task_id: ti.state for ti in dag_run.get_task_instances(session=session)}
    assert dag_run.state == DagRunState.SUCCESS
    assert all(state == TaskInstanceState.SUCCESS for state in states.values()), states
    return states


def assert_warehouse_objects(warehouse: Warehouse) -> None:
    with warehouse.connect() as connection, connection.cursor() as cursor:
        assert warehouse.is_semantic_object(cursor)
        assert warehouse.revenue_from_semantic_object(cursor) == SEEDED_REVENUE_BY_STATUS
        assert warehouse.revenue_from_downstream_table(cursor) == SEEDED_REVENUE_BY_STATUS


def asset_event_uris_by_task(dag_id: str) -> dict[str, list[str]]:
    from airflow.models.asset import AssetEvent

    uris: dict[str, list[str]] = defaultdict(list)
    with create_session() as session:
        for event in session.query(AssetEvent).filter(AssetEvent.source_dag_id == dag_id).all():
            uris[event.source_task_id].append(event.asset.uri)
    return dict(uris)


def disable_full_refresh(dag: DbtDag) -> None:
    refreshable_tasks = [task for task in dag.tasks if hasattr(task, "full_refresh")]
    # Seed consumers in watcher mode lose full_refresh (DbtRunMixin.__init__ runs after DbtSeedMixin.__init__ and
    # resets it), so only the run tasks and the producer are expected to carry the flag before the toggle.
    expected_refreshing = {
        task.task_id for task in refreshable_tasks if task.task_id.endswith(("_run", ".run", "dbt_producer_watcher"))
    }
    refreshing = {task.task_id for task in refreshable_tasks if "--full-refresh" in task.add_cmd_flags()}
    assert expected_refreshing <= refreshing, expected_refreshing - refreshing
    for task in refreshable_tasks:
        task.full_refresh = False
        assert "--full-refresh" not in task.add_cmd_flags(), task.task_id


def run_full_refresh_then_rerun(dag: DbtDag, warehouse: Warehouse) -> None:
    run_dag(dag)
    assert_warehouse_objects(warehouse)
    disable_full_refresh(dag)
    run_dag(dag)
    assert_warehouse_objects(warehouse)


def semantic_task_id_in(chain: list[str]) -> str:
    return next(task_id for task_id in chain if "semantic_metric_run" in task_id)


@pytest.fixture(scope="module")
def local_dag(warehouse: Warehouse) -> DbtDag:
    dag = load_example_dag(warehouse, "local")
    run_full_refresh_then_rerun(dag, warehouse)
    return dag


@pytest.fixture(scope="module")
def watcher_dag(warehouse: Warehouse) -> DbtDag:
    dag = load_example_dag(warehouse, "watcher")
    make_watcher_consumers_wait_for_producer(dag)
    run_full_refresh_then_rerun(dag, warehouse)
    return dag


def test_dbt_dag_with_local_execution(warehouse: Warehouse, local_dag: DbtDag):
    chain = expected_task_chain(warehouse)
    seed_task_id, orders_task_id = chain[0], chain[1]
    semantic_task_id = semantic_task_id_in(chain)

    assert len(local_dag.dbt_graph.filtered_nodes) == EXPECTED_NODE_COUNT
    assert topological_task_ids(local_dag) == chain
    assert isinstance(local_dag.task_dict[seed_task_id], DbtSeedLocalOperator)
    assert isinstance(local_dag.task_dict[orders_task_id], DbtRunLocalOperator)
    assert isinstance(local_dag.task_dict[chain[-1]], DbtTestLocalOperator)
    semantic_task = local_dag.task_dict[semantic_task_id]
    assert isinstance(semantic_task, DbtSemanticMetricViewLocalOperator)
    assert semantic_task.ui_color == DbtSemanticMetricViewMixin.ui_color
    assert semantic_task.base_cmd == ["run"]
    assert semantic_task.select == f"fqn:{warehouse.project_name}.{warehouse.semantic_model}"
    assert semantic_task.upstream_task_ids == {chain[chain.index(semantic_task_id) - 1]}
    assert semantic_task.downstream_task_ids == {chain[chain.index(semantic_task_id) + 1]}
    assert "--full-refresh" not in semantic_task.add_cmd_flags()
    assert_warehouse_objects(warehouse)


def test_dbt_dag_with_watcher_execution(warehouse: Warehouse, watcher_dag: DbtDag):
    chain = expected_task_chain(warehouse)
    seed_task_id, orders_task_id = chain[0], chain[1]
    semantic_task_id = semantic_task_id_in(chain)
    tests_excluded_from_producer = warehouse.test_behavior != TestBehavior.AFTER_EACH

    assert len(watcher_dag.dbt_graph.filtered_nodes) == EXPECTED_NODE_COUNT
    assert topological_task_ids(watcher_dag) == ["dbt_producer_watcher", *chain]
    producer = watcher_dag.task_dict["dbt_producer_watcher"]
    assert isinstance(producer, DbtProducerWatcherOperator)
    assert isinstance(watcher_dag.task_dict[seed_task_id], DbtSeedWatcherOperator)
    assert isinstance(watcher_dag.task_dict[orders_task_id], DbtRunWatcherOperator)
    assert isinstance(watcher_dag.task_dict[semantic_task_id], DbtSemanticMetricViewWatcherOperator)
    # With tests excluded from the producer, the trailing test task is a plain local dbt test, not a sensor.
    expected_test_operator = DbtTestLocalOperator if tests_excluded_from_producer else DbtTestWatcherOperator
    assert isinstance(watcher_dag.task_dict[chain[-1]], expected_test_operator)
    assert "dbt_producer_watcher" in watcher_dag.task_dict[seed_task_id].upstream_task_ids

    # The producer narrows `dbt build` with dbt resource types; dbt still classifies these nodes as
    # models, so Cosmos's own type must never appear in the command.
    command, _ = producer.build_cmd({}, producer.add_cmd_flags())
    flags = [str(part) for part in command]
    resource_types = {flags[index + 1] for index, part in enumerate(flags) if part == "--resource-type"}
    assert resource_types == ({"model", "snapshot", "seed"} if tests_excluded_from_producer else set())
    assert DbtResourceType.SEMANTIC_METRIC_VIEW.value not in flags
    assert "--full-refresh" not in flags
    assert_warehouse_objects(warehouse)


@pytest.mark.skipif(
    AIRFLOW_VERSION.major < 3, reason="Asset events are read from airflow.models.asset, available from Airflow 3"
)
@pytest.mark.parametrize("dag_fixture", ["local_dag", "watcher_dag"])
def test_semantic_metric_view_task_emits_asset_events(
    warehouse: Warehouse, dag_fixture: str, request: pytest.FixtureRequest
):
    dag = request.getfixturevalue(dag_fixture)
    chain = expected_task_chain(warehouse)
    events = asset_event_uris_by_task(dag.dag_id)
    assert events.get(chain[1]), events
    assert events.get(semantic_task_id_in(chain)), events
