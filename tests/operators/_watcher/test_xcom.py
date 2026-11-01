"""Unit tests for the Airflow Variable compatibility helpers used by the WATCHER XCom backup."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from cosmos.airflow import compatibility
from cosmos.constants import AIRFLOW_VERSION
from cosmos.operators._watcher.state import safe_xcom_push
from cosmos.operators._watcher.xcom import (
    TASK_STATE_STORE_TI_ATTRIBUTE,
    _backup_xcom_to_variable,
    _decode_backup,
    _init_xcom_backup,
    _persist_backup,
    _restore_xcom_backup,
    _task_state_store_row_key,
    _xcom_backup_variable_key,
)


@patch("cosmos.operators._watcher.xcom.set_variable")
def test_persist_backup_uses_set_variable(mock_set_variable):
    _persist_backup("cosmos_xcom_backup__key", {"foo": "bar"})

    mock_set_variable.assert_called_once()


def test_get_variable_prefers_sdk():
    sdk_variable = MagicMock()
    with patch.dict(sys.modules, {"airflow.sdk": MagicMock(Variable=sdk_variable)}):
        compatibility.get_variable("k", default="x")

    sdk_variable.get.assert_called_once_with("k", default="x")


def test_set_variable_falls_back_to_models_outside_task_runner():
    sdk_variable = MagicMock()
    sdk_variable.set.side_effect = ImportError("SUPERVISOR_COMMS")
    models_variable = MagicMock()
    with (
        patch.dict(sys.modules, {"airflow.sdk": MagicMock(Variable=sdk_variable)}),
        patch("airflow.models.Variable", models_variable),
    ):
        compatibility.set_variable("k", "v")

    models_variable.set.assert_called_once_with("k", "v")


def test_get_variable_falls_back_to_models_outside_task_runner():
    sdk_variable = MagicMock()
    sdk_variable.get.side_effect = ImportError("SUPERVISOR_COMMS")
    models_variable = MagicMock()
    with (
        patch.dict(sys.modules, {"airflow.sdk": MagicMock(Variable=sdk_variable)}),
        patch("airflow.models.Variable", models_variable),
    ):
        compatibility.get_variable("k", default="x")

    models_variable.get.assert_called_once_with("k", default_var="x")


@pytest.mark.integration
@pytest.mark.skipif(AIRFLOW_VERSION < Version("3.3.0"), reason="The Airflow task store was added in Airflow 3.3")
def test_xcom_backup_survives_retry_in_task_state_store():
    from airflow.sdk import DAG, task

    from tests.utils import new_test_dag

    observed = {}

    @task(retries=1, retry_delay=timedelta(seconds=0))
    def producer(**context):
        ti = context["ti"]
        if ti.try_number == 1:
            _init_xcom_backup(context)
            safe_xcom_push(ti, "model__a_status", "success")
            observed["persisted"] = context["task_state_store"].get(_task_state_store_row_key(0)) is not None
            raise RuntimeError("fail the first attempt")
        observed["restored"] = _restore_xcom_backup(context)
        observed["status"] = ti.xcom_pull(task_ids=ti.task_id, key="model__a_status")
        observed["leftover"] = context["task_state_store"].get(_task_state_store_row_key(0))

    with DAG("cosmos_xcom_backup_task_state_store", start_date=datetime(2024, 1, 1), schedule=None) as dag:
        producer()

    dag_run = new_test_dag(dag)

    assert observed == {"persisted": True, "restored": True, "status": "success", "leftover": None}
    variable_key = _xcom_backup_variable_key(dag.dag_id, None, dag_run.run_id)
    assert compatibility.get_variable(variable_key, default=None) is None


class _StoreFailingTheLastWrite:
    def __init__(self, store, last_key: str):
        self._store = store
        self._last_key = last_key

    def __getattr__(self, name):
        return getattr(self._store, name)

    def set(self, key, value):
        if self._last_key in _decode_backup(value):
            raise RuntimeError("store unavailable")
        self._store.set(key, value)


@pytest.mark.integration
@pytest.mark.skipif(AIRFLOW_VERSION < Version("3.3.0"), reason="The Airflow task store was added in Airflow 3.3")
def test_xcom_backup_falls_back_to_the_variable_when_the_last_write_fails():
    from airflow.sdk import DAG, task

    from tests.utils import new_test_dag

    statuses = ("model__a_status", "model__b_status", "model__c_status")
    observed = {}

    @task(retries=1, retry_delay=timedelta(seconds=0), on_retry_callback=_backup_xcom_to_variable)
    def producer(**context):
        ti = context["ti"]
        if ti.try_number == 1:
            _init_xcom_backup(context)
            setattr(
                ti, TASK_STATE_STORE_TI_ATTRIBUTE, _StoreFailingTheLastWrite(context["task_state_store"], statuses[-1])
            )
            for key in statuses:
                safe_xcom_push(ti, key, "success")
        observed["restored"] = _restore_xcom_backup(context)
        observed["statuses"] = {key: ti.xcom_pull(task_ids=ti.task_id, key=key) for key in statuses}
        observed["leftover"] = context["task_state_store"].get(_task_state_store_row_key(0))

    with DAG("cosmos_xcom_backup_variable_fallback", start_date=datetime(2024, 1, 1), schedule=None) as dag:
        producer()

    dag_run = new_test_dag(dag)

    assert observed == {"restored": True, "statuses": dict.fromkeys(statuses, "success"), "leftover": None}
    variable_key = _xcom_backup_variable_key(dag.dag_id, None, dag_run.run_id)
    assert compatibility.get_variable(variable_key, default=None) is None


@pytest.mark.integration
def test_xcom_backup_survives_retry_in_the_variable_when_not_persisting():
    if AIRFLOW_VERSION >= Version("3.1.0"):
        from airflow.sdk import DAG, task
    else:
        from airflow import DAG
        from airflow.decorators import task

    from tests.utils import new_test_dag

    observed = {}

    @task(retries=1, retry_delay=timedelta(seconds=0), on_retry_callback=_backup_xcom_to_variable)
    def producer(**context):
        ti = context["ti"]
        if ti.try_number == 1:
            _init_xcom_backup(context, persist=False)
            safe_xcom_push(ti, "model__a_status", "success")
            raise RuntimeError("fail the first attempt")
        task_state_store = context.get("task_state_store")
        observed["store_untouched"] = (
            task_state_store is None or task_state_store.get(_task_state_store_row_key(0)) is None
        )
        observed["restored"] = _restore_xcom_backup(context, read_task_state_store=False)
        observed["status"] = ti.xcom_pull(task_ids=ti.task_id, key="model__a_status")

    with DAG("cosmos_xcom_backup_variable_only", start_date=datetime(2024, 1, 1), schedule=None) as dag:
        producer()

    dag_run = new_test_dag(dag)

    assert observed == {"store_untouched": True, "restored": True, "status": "success"}
    variable_key = _xcom_backup_variable_key(dag.dag_id, None, dag_run.run_id)
    assert compatibility.get_variable(variable_key, default=None) is None
