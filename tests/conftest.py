import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection
from packaging.version import Version
from sqlalchemy import Table as SQLAlchemyTable

from cosmos.constants import AIRFLOW_VERSION

# Workaround for Airflow 3.0/3.1: avoid "Table 'dag_warning' is already defined for this
# MetaData instance" when tests trigger duplicate table registration (e.g. DagBag, sync_bag_to_db).
if AIRFLOW_VERSION >= Version("3.0"):
    _original_table_init = SQLAlchemyTable.__init__

    def _patched_table_init(self, *args, **kwargs):
        if args and args[0] == "dag_warning":
            kwargs["extend_existing"] = True
        return _original_table_init(self, *args, **kwargs)

    SQLAlchemyTable.__init__ = _patched_table_init

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
