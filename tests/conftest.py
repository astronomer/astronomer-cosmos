import json
import logging
import os
from unittest.mock import Mock, patch

import pytest
from airflow import __version__ as airflow_version
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION
from cosmos.log import CosmosRichLogger

# dbt v2 does not support the Postgres adapter the integration tests are written for, so the dbt v2 CI jobs run
# them against BigQuery: when COSMOS_TEST_ADAPTER=bigquery, every PostgresUserPasswordProfileMapping built by the
# tests and the example DAGs becomes a BigQuery mapping that writes to COSMOS_TEST_BIGQUERY_DATASET.
COSMOS_TEST_ADAPTER = os.getenv("COSMOS_TEST_ADAPTER")

# Tests that depend on Postgres itself (and not only on the profile) and so cannot run against BigQuery.
POSTGRES_ONLY_TEST_FILES = {
    "tests/listeners/test_dag_run_listener.py": "asserts on the Postgres profile mapping",
    "tests/operators/test_virtualenv.py": "installs dbt-postgres in a virtualenv",
}
POSTGRES_ONLY_FIXTURES = {
    "reset_fail_once_sequence": "uses psycopg2 and Postgres sequences",
}


def _use_bigquery_in_place_of_postgres() -> None:
    import cosmos.profiles
    import cosmos.profiles.postgres
    from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

    class BigQueryInPlaceOfPostgresProfileMapping(GoogleCloudServiceAccountDictProfileMapping):
        def __init__(self, conn_id, profile_args=None, disable_event_tracking=None, dbt_config_vars=None):
            bigquery_profile_args = {k: v for k, v in (profile_args or {}).items() if k != "schema"}
            bigquery_profile_args["dataset"] = os.environ["COSMOS_TEST_BIGQUERY_DATASET"]
            bigquery_profile_args["project"] = os.getenv("COSMOS_TEST_BIGQUERY_PROJECT", "astronomer-dag-authoring")
            super().__init__(
                conn_id="gcp_gs_conn",
                profile_args=bigquery_profile_args,
                disable_event_tracking=disable_event_tracking,
                dbt_config_vars=dbt_config_vars,
            )

    cosmos.profiles.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping
    cosmos.profiles.postgres.PostgresUserPasswordProfileMapping = BigQueryInPlaceOfPostgresProfileMapping


if COSMOS_TEST_ADAPTER == "bigquery":
    _use_bigquery_in_place_of_postgres()


def pytest_collection_modifyitems(items):
    if COSMOS_TEST_ADAPTER != "bigquery":
        return
    for item in items:
        file_path = item.nodeid.split("::")[0]
        reason = POSTGRES_ONLY_TEST_FILES.get(file_path) or next(
            (why for fixture, why in POSTGRES_ONLY_FIXTURES.items() if fixture in item.fixturenames), None
        )
        if reason:
            item.add_marker(pytest.mark.skip(reason=f"Requires Postgres: {reason}"))


@pytest.fixture(autouse=True, scope="session")
def _cache_airflow_in_process_api():
    """Cache the InProcessExecutionAPI to avoid per-task FastAPI app creation in dag.test().

    Airflow 3.x's dag.test() creates a new InProcessExecutionAPI — a full FastAPI
    application with ASGI middleware, JWT auth, dependency injection, and an async
    event loop — for every single task. This adds ~6-8s of overhead per task,
    making a 13-task DAG take ~80s instead of ~2s.

    This fixture patches in_process_api_server() to return a cached instance,
    so the FastAPI app is created once and reused across all tasks and tests.
    """
    if AIRFLOW_VERSION < Version("3.1"):
        yield
        return

    try:
        from airflow.sdk.execution_time import supervisor as supervisor_module

        _original_fn = supervisor_module.in_process_api_server
    except (ImportError, AttributeError):
        yield
        return

    _cached_api = None

    def cached_in_process_api_server():
        nonlocal _cached_api
        if _cached_api is None:
            _cached_api = _original_fn()
        return _cached_api

    supervisor_module.in_process_api_server = cached_in_process_api_server
    try:
        yield
    finally:
        supervisor_module.in_process_api_server = _original_fn


@pytest.fixture(autouse=True)
def _cleanup_rich_loggers():
    """Replace any CosmosRichLogger instances with standard loggers after each test.

    get_logger() registers CosmosRichLogger instances in Python's global logging cache.
    If a cosmos module is imported for the first time while rich_logging=True
    (via monkeypatch in test_log.py), its module-level logger becomes a CosmosRichLogger
    permanently, polluting subsequent tests with the (astronomer-cosmos) prefix.
    """
    yield
    manager = logging.Logger.manager
    for name in list(manager.loggerDict):
        obj = manager.loggerDict[name]
        if isinstance(obj, CosmosRichLogger):
            obj.__class__ = logging.Logger


def make_task_instance(task, **kwargs):
    """
    Create a TaskInstance from an operator, compatible with Airflow 3.2+ weight_rule API.
    In 3.2+, WeightRule no longer has get_weight; ensure the task has a compatible weight_rule
    before TaskInstance(...) calls refresh_from_task().
    """
    if not getattr(task.weight_rule, "get_weight", None):
        task.weight_rule = Mock(get_weight=lambda ti: getattr(task, "priority_weight", 1))
    if Version(airflow_version) >= Version("3.1"):
        return TaskInstance(task, dag_version_id=None, **kwargs)
    return TaskInstance(task, **kwargs)


if AIRFLOW_VERSION >= Version("3.1"):
    # Change introduced in Airflow 3.1.0
    # https://github.com/apache/airflow/pull/55722/files
    base_operator_get_connection_path = "airflow.sdk.BaseHook.get_connection"
else:
    base_operator_get_connection_path = "airflow.hooks.base.BaseHook.get_connection"


@pytest.fixture()
def mock_bigquery_conn():  # type: ignore
    """
    Mocks and returns an Airflow BigQuery connection.
    """
    extra = {"project": "my_project", "key_path": "my_key_path.json", "dataset": "test"}
    conn = Connection(
        conn_id="my_bigquery_connection",
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )

    with patch(base_operator_get_connection_path, return_value=conn):
        yield conn
