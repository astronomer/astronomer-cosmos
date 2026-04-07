import json
from unittest.mock import Mock, patch

import pytest
from airflow import __version__ as airflow_version
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION


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
