import logging
from unittest.mock import patch

import httpx
import pytest

from cosmos import telemetry


def test_should_emit_is_true_by_default():
    assert telemetry.should_emit()


@patch("cosmos.settings.enable_telemetry", True)
def test_should_emit_is_true_when_only_enable_telemetry_is_true():
    assert telemetry.should_emit()


@patch("cosmos.settings.do_not_track", True)
def test_should_emit_is_false_when_do_not_track():
    assert not telemetry.should_emit()


@patch("cosmos.settings.no_analytics", True)
def test_should_emit_is_false_when_no_analytics():
    assert not telemetry.should_emit()


def test_collect_standard_usage_metrics():
    metrics = telemetry.collect_standard_usage_metrics()
    expected_keys = [
        "airflow_version",
        "cosmos_version",
        "platform_machine",
        "platform_system",
        "python_version",
    ]
    assert sorted(metrics.keys()) == expected_keys


class MockFailedResponse:
    is_success = False
    status_code = "404"
    text = "Non existent URL"


@patch("cosmos.telemetry.httpx.get", return_value=MockFailedResponse())
def test_emit_usage_metrics_is_unsuccessful(mock_httpx_get, caplog):
    sample_metrics = {
        "cosmos_version": "1.8.0a4",
        "airflow_version": "2.10.1",
        "python_version": "3.11",
        "platform_system": "darwin",
        "platform_machine": "amd64",
        "event_type": "dag_run",
        "status": "success",
        "dag_hash": "d151d1fa2f03270ea116cc7494f2c591",
        "task_count": 3,
        "cosmos_task_count": 3,
        "execution_modes": "local",
    }
    is_success = telemetry.emit_usage_metrics(sample_metrics)
    mock_httpx_get.assert_called_once_with(
        f"""https://astronomer.gateway.scarf.sh/astronomer-cosmos/v3/dag_run?cosmos_version=1.8.0a4&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&status=success&dag_hash=d151d1fa2f03270ea116cc7494f2c591&task_count=3&cosmos_task_count=3&execution_modes=local""",
        timeout=1.0,
        follow_redirects=True,
    )
    assert not is_success
    log_msg = f"""Unable to emit usage metrics to https://astronomer.gateway.scarf.sh/astronomer-cosmos/v3/dag_run?cosmos_version=1.8.0a4&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&status=success&dag_hash=d151d1fa2f03270ea116cc7494f2c591&task_count=3&cosmos_task_count=3&execution_modes=local. Status code: 404. Message: Non existent URL"""
    assert "WARNING" in caplog.text
    assert log_msg in caplog.text


@patch("cosmos.telemetry.httpx.get", side_effect=httpx.ConnectError(message="Something is not right"))
def test_emit_usage_metrics_fails(mock_httpx_get, caplog):
    sample_metrics = {
        "cosmos_version": "1.8.0a4",
        "airflow_version": "2.10.1",
        "python_version": "3.11",
        "platform_system": "darwin",
        "platform_machine": "amd64",
        "event_type": "dag_run",
        "status": "success",
        "dag_hash": "d151d1fa2f03270ea116cc7494f2c591",
        "task_count": 3,
        "cosmos_task_count": 3,
        "execution_modes": "local",
    }
    is_success = telemetry.emit_usage_metrics(sample_metrics)
    mock_httpx_get.assert_called_once_with(
        f"""https://astronomer.gateway.scarf.sh/astronomer-cosmos/v3/dag_run?cosmos_version=1.8.0a4&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&status=success&dag_hash=d151d1fa2f03270ea116cc7494f2c591&task_count=3&cosmos_task_count=3&execution_modes=local""",
        timeout=1.0,
        follow_redirects=True,
    )
    assert not is_success
    log_msg = f"""Unable to emit usage metrics to https://astronomer.gateway.scarf.sh/astronomer-cosmos/v3/dag_run?cosmos_version=1.8.0a4&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&status=success&dag_hash=d151d1fa2f03270ea116cc7494f2c591&task_count=3&cosmos_task_count=3&execution_modes=local. An HTTPX connection error occurred: Something is not right."""
    assert "WARNING" in caplog.text
    assert log_msg in caplog.text


@pytest.mark.integration
@pytest.mark.flaky(reruns=2, reruns_delay=1)
def test_emit_usage_metrics_succeeds(caplog):
    caplog.set_level(logging.DEBUG)
    sample_metrics = {
        "cosmos_version": "1.8.0a4",
        "airflow_version": "2.10.1",
        "python_version": "3.11",
        "platform_system": "darwin",
        "platform_machine": "amd64",
        "event_type": "dag_run",
        "status": "success",
        "dag_hash": "dag-hash-ci",
        "task_count": 33,
        "cosmos_task_count": 33,
        "execution_modes": "local",
    }
    is_success = telemetry.emit_usage_metrics(sample_metrics)
    assert is_success
    assert caplog.text.startswith("DEBUG")
    assert "Telemetry is enabled. Emitting the following usage metrics for event type" in caplog.text


@patch("cosmos.telemetry.should_emit", return_value=False)
def test_emit_usage_metrics_if_enabled_fails(mock_should_emit, caplog):
    caplog.set_level(logging.DEBUG)
    assert not telemetry.emit_usage_metrics_if_enabled("any", {})
    assert "DEBUG" in caplog.text
    assert "Telemetry is disabled. To enable it, export AIRFLOW__COSMOS__ENABLE_TELEMETRY=True." in caplog.text


@patch("cosmos.telemetry.should_emit", return_value=True)
@patch("cosmos.telemetry.collect_standard_usage_metrics", return_value={"k1": "v1", "k2": "v2"})
@patch("cosmos.telemetry.emit_usage_metrics")
def test_emit_usage_metrics_if_enabled_succeeds(
    mock_emit_usage_metrics, mock_collect_standard_usage_metrics, mock_should_emit
):
    assert telemetry.emit_usage_metrics_if_enabled("any", {"k2": "v2"})
    mock_emit_usage_metrics.assert_called_once()
    assert mock_emit_usage_metrics.call_args.args[0] == {
        "k1": "v1",
        "k2": "v2",
        "event_type": "any",
    }
