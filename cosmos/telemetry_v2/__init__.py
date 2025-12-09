from __future__ import annotations

from typing import Dict, Iterable

import cosmos
from cosmos import settings
from cosmos.log import get_logger
from cosmos.telemetry_v2.base import TelemetryManager
from cosmos.telemetry_v2.manager import build_default_telemetry_manager

logger = get_logger(__name__)


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

    logger.info("Emitting usage event: %s", metrics)

    telemetry_manager = build_default_telemetry_manager(settings.telemetry_backends or {"scarf"})
    return telemetry_manager.emit_usage_event(event_type, metrics)


def should_emit() -> bool:
    return True
    # return settings.enable_telemetry and not settings.do_not_track and not settings.no_analytics


from cosmos.telemetry_v2.collector import (
    collect_task_metrics,
    emit_database_metric,
    emit_dbt_command_metric,
    emit_dbt_ls_invocation_mode_metric,
    emit_execution_mode_metric,
    emit_load_mode_metric,
    emit_local_invocation_mode_metric,
    emit_operator_metric,
    emit_parsing_duration_metric,
    emit_profile_method_metrics,
    emit_project_size_metric,
    emit_subclass_metric,
    emit_task_duration_metric,
    emit_task_origin_metric,
    emit_task_status_metric,
)

__all__ = [
    "emit_usage_event",
    "should_emit",
    "collect_task_metrics",
    "emit_operator_metric",
    "emit_subclass_metric",
    "emit_dbt_command_metric",
    "emit_execution_mode_metric",
    "emit_database_metric",
    "emit_profile_method_metrics",
    "emit_task_origin_metric",
    "emit_task_status_metric",
    "emit_local_invocation_mode_metric",
    "emit_load_mode_metric",
    "emit_dbt_ls_invocation_mode_metric",
    "emit_parsing_duration_metric",
    "emit_project_size_metric",
    "emit_task_duration_metric",
]
