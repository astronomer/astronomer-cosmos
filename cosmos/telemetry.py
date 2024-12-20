from __future__ import annotations

import platform
from urllib import parse
from urllib.parse import urlencode

import httpx
from airflow import __version__ as airflow_version

import cosmos
from cosmos import constants, settings
from cosmos.log import get_logger

logger = get_logger(__name__)


def should_emit() -> bool:
    """
    Identify if telemetry metrics should be emitted or not.
    """
    return settings.enable_telemetry and not settings.do_not_track and not settings.no_analytics


def collect_standard_usage_metrics() -> dict[str, object]:
    """
    Return standard telemetry metrics.
    """
    metrics = {
        "cosmos_version": cosmos.__version__,  # type: ignore[attr-defined]
        "airflow_version": parse.quote(airflow_version),
        "python_version": platform.python_version(),
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "variables": {},
    }
    return metrics


def emit_usage_metrics(metrics: dict[str, object]) -> bool:
    """
    Emit desired telemetry metrics to remote telemetry endpoint.

    The metrics must contain the necessary fields to build the TELEMETRY_URL.
    """
    query_string = urlencode(metrics)
    telemetry_url = constants.TELEMETRY_URL.format(
        **metrics, telemetry_version=constants.TELEMETRY_VERSION, query_string=query_string
    )
    logger.debug("Telemetry is enabled. Emitting the following usage metrics to %s: %s", telemetry_url, metrics)
    response = httpx.get(telemetry_url, timeout=constants.TELEMETRY_TIMEOUT, follow_redirects=True)
    if not response.is_success:
        logger.warning(
            "Unable to emit usage metrics to %s. Status code: %s. Message: %s",
            telemetry_url,
            response.status_code,
            response.text,
        )
    return response.is_success


def emit_usage_metrics_if_enabled(event_type: str, additional_metrics: dict[str, object]) -> bool:
    """
    Checks if telemetry should be emitted, fetch standard metrics, complement with custom metrics
    and emit them to remote telemetry endpoint.

    :returns: If the event was successfully sent to the telemetry backend or not.
    """
    if should_emit():
        metrics = collect_standard_usage_metrics()
        metrics["event_type"] = event_type
        metrics["variables"].update(additional_metrics)  # type: ignore[attr-defined]
        metrics.update(additional_metrics)
        is_success = emit_usage_metrics(metrics)
        return is_success
    else:
        logger.debug("Telemetry is disabled. To enable it, export AIRFLOW__COSMOS__ENABLE_TELEMETRY=True.")
        return False
