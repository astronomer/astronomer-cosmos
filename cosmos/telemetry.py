from __future__ import annotations

import platform
from urllib import parse

import cosmos
from airflow import __version__ as airflow_version

from cosmos.telemetry.manager import build_default_telemetry_manager
from cosmos.telemetry.base import TelemetryMetric
from cosmos import settings


def should_emit() -> bool:
    return settings.enable_telemetry and not settings.do_not_track and not settings.no_analytics


def collect_standard_usage_metrics() -> dict[str, object]:
    metrics: dict[str, object] = {
        "cosmos_version": cosmos.__version__,
        "airflow_version": parse.quote(airflow_version),
        "python_version": platform.python_version(),
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "variables": {},
    }
    return metrics


def emit_usage_metrics(metrics: dict[str, object]) -> bool:
    manager = build_default_telemetry_manager(settings.telemetry_backends or {"scarf"})
    event_type = metrics.get("event_type", "unknown")
    return manager.emit_usage_event(event_type, metrics)


def emit_usage_metrics_if_enabled(event_type: str, additional_metrics: dict[str, object]) -> bool:
    if not should_emit():
        return False

    metrics = collect_standard_usage_metrics()
    metrics["event_type"] = event_type
    metrics["variables"].update(additional_metrics)
    metrics.update(additional_metrics)

    return emit_usage_metrics(metrics)


def emit_metric(metric: TelemetryMetric) -> None:
    manager = build_default_telemetry_manager(settings.telemetry_backends or {"scarf"})
    manager.emit_metric(metric)
