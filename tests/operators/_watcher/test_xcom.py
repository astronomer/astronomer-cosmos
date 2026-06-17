"""Unit tests for the Airflow Variable compatibility helpers used by the WATCHER XCom backup."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from cosmos.airflow import compatibility
from cosmos.operators._watcher.xcom import _persist_backup


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
