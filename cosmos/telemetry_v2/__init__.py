from __future__ import annotations

from typing import Dict, Iterable

import cosmos
from cosmos import settings
from cosmos.telemetry_v2.base import TelemetryManager
from cosmos.telemetry_v2.manager import build_default_telemetry_manager


def build_usage_payload(additional_metrics: Dict[str, object]) -> Dict[str, object]:
    metrics: Dict[str, object] = {
        "cosmos_version": cosmos.__version__,  # type: ignore[attr-defined]
        "variables": {},
    }
    metrics.update(additional_metrics)
    metrics["variables"].update(additional_metrics)
    return metrics


def emit_usage_event(event_type: str, additional_metrics: Dict[str, object]) -> bool:
    if not should_emit():
        return False

    metrics = build_usage_payload(additional_metrics)
    metrics["event_type"] = event_type

    telemetry_manager = build_default_telemetry_manager(settings.telemetry_backends or {"scarf"})
    return telemetry_manager.emit_usage_event(event_type, metrics)


def should_emit() -> bool:
    return settings.enable_telemetry and not settings.do_not_track and not settings.no_analytics


__all__ = ["emit_usage_event", "should_emit"]
