from __future__ import annotations

from typing import Dict

from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

from cosmos.telemetry_v2.base import MetricType, TelemetryEmitter, TelemetryMetric, sanitize_metric_name, sanitize_tags


class StatsdTelemetryEmitter(TelemetryEmitter, LoggingMixin):
    """Emit telemetry metrics via Airflow's configured StatsD client."""

    def emit_metric(self, metric: TelemetryMetric) -> None:
        name = sanitize_metric_name(metric.name)
        tags = sanitize_tags(metric.tags)
        tag_suffix = ".".join(f"{k}.{v}" for k, v in sorted(tags.items()))
        metric_name = f"{name}.{tag_suffix}" if tag_suffix else name

        if metric.metric_type == MetricType.COUNTER:
            Stats.incr(metric_name, count=int(metric.value))
        elif metric.metric_type == MetricType.TIMER:
            Stats.timing(metric_name, metric.value)
        elif metric.metric_type == MetricType.GAUGE:
            Stats.gauge(metric_name, metric.value)
        else:  # pragma: no cover - defensive
            self.log.debug("Unsupported metric type %s for %s", metric.metric_type, metric_name)

    def emit_usage_event(self, event_type: str, payload: Dict[str, object]) -> None:
        # StatsD does not support structured events; ignore
        return

    def flush(self) -> None:
        return
