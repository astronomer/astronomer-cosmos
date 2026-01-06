from __future__ import annotations

import gzip
import json
import platform
from base64 import b64decode, b64encode
from typing import Any
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
    }
    return metrics


def emit_usage_metrics(metrics: dict[str, object]) -> bool:
    """
    Emit desired telemetry metrics to remote telemetry endpoint.

    The metrics must contain the necessary fields to build the TELEMETRY_URL.
    """
    event_type = metrics.get("event_type")
    metrics_for_query = {k: v for k, v in metrics.items() if k != "event_type"}
    query_string = urlencode(metrics_for_query)
    telemetry_url = constants.TELEMETRY_URL.format(
        telemetry_version=constants.TELEMETRY_VERSION, event_type=event_type, query_string=query_string
    )
    logger.debug(
        "Telemetry is enabled. Emitting the following usage metrics for event type %s to %s: %s",
        event_type,
        telemetry_url,
        metrics,
    )
    try:
        response = httpx.get(telemetry_url, timeout=constants.TELEMETRY_TIMEOUT, follow_redirects=True)
    except httpx.HTTPError as e:
        logger.warning(
            "Unable to emit usage metrics to %s. An HTTPX connection error occurred: %s.", telemetry_url, str(e)
        )
        is_success = False
    else:
        is_success = response.is_success
        if not is_success:
            logger.warning(
                "Unable to emit usage metrics to %s. Status code: %s. Message: %s",
                telemetry_url,
                response.status_code,
                response.text,
            )
    return is_success


def emit_usage_metrics_if_enabled(event_type: str, additional_metrics: dict[str, object]) -> bool:
    """
    Checks if telemetry should be emitted, fetch standard metrics, complement with custom metrics
    and emit them to remote telemetry endpoint.

    :returns: If the event was successfully sent to the telemetry backend or not.
    """
    if should_emit():
        metrics = collect_standard_usage_metrics()
        metrics["event_type"] = event_type
        metrics.update(additional_metrics)
        is_success = emit_usage_metrics(metrics)
        return is_success
    else:
        logger.debug("Telemetry is disabled. To enable it, export AIRFLOW__COSMOS__ENABLE_TELEMETRY=True.")
        return False


def _compress_telemetry_metadata(metadata: dict[str, Any]) -> str:
    """
    Compress and encode telemetry metadata to reduce serialized DAG size.

    Uses mtime=0 in gzip compression to ensure deterministic output regardless of when
    the compression occurs. This prevents spurious Airflow Param validation errors that
    would occur if the same metadata compressed at different times produced different
    base64 strings.

    :param metadata: Telemetry metadata dictionary
    :returns: Base64-encoded gzip-compressed JSON string
    """
    json_bytes = json.dumps(metadata).encode("utf-8")
    compressed = gzip.compress(json_bytes, compresslevel=9, mtime=0)
    return b64encode(compressed).decode("ascii")


def _decompress_telemetry_metadata(compressed_data: str) -> dict[str, Any]:
    """
    Decompress and decode telemetry metadata.

    :param compressed_data: Base64-encoded gzip-compressed JSON string
    :returns: Original metadata dictionary
    """
    compressed_bytes = b64decode(compressed_data.encode("ascii"))
    json_bytes = gzip.decompress(compressed_bytes)
    result: dict[str, Any] = json.loads(json_bytes.decode("utf-8"))
    return result
