"""Shared telemetry data structures and interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, Optional


class MetricType(str, Enum):
    COUNTER = "counter"
    TIMER = "timer"
    GAUGE = "gauge"


@dataclass(frozen=True)
class TelemetryMetric:
    """A single numeric measurement to emit to one or more telemetry backends."""

    name: str
    metric_type: MetricType
    value: float = 1.0
    tags: Optional[Dict[str, str]] = None


class TelemetryEmitter:
    """Interface for telemetry backends."""

    def emit_metric(self, metric: TelemetryMetric) -> None:
        raise NotImplementedError

    def emit_usage_event(self, event_type: str, payload: Dict[str, object]) -> bool:
        """Emit a structured usage event.

        Returns ``True`` when the backend accepted the event, ``False`` otherwise.
        Metrics-only backends can return ``False`` to signal the event was ignored.
        """
        raise NotImplementedError

    def flush(self) -> None:
        """Allow backends to flush buffers after a batch of metrics."""


class NullTelemetryEmitter(TelemetryEmitter):
    """No-op emitter used when a backend is disabled."""

    def emit_metric(self, metric: TelemetryMetric) -> None:  # pragma: no cover - trivial
        return

    def emit_usage_event(self, event_type: str, payload: Dict[str, object]) -> bool:  # pragma: no cover - trivial
        return False

    def flush(self) -> None:  # pragma: no cover - trivial
        return


def sanitize_metric_name(name: str) -> str:
    """Normalise metric identifiers for backends that prefer snake_case."""

    safe = []
    previous_was_separator = False
    for char in name:
        if char.isalnum():
            safe.append(char.lower())
            previous_was_separator = False
        elif not previous_was_separator:
            safe.append("_")
            previous_was_separator = True
    trimmed = "".join(safe).strip("_")
    return trimmed or "metric"


def sanitize_tags(tags: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not tags:
        return {}
    return {sanitize_metric_name(k): str(v) for k, v in tags.items()}


class TelemetryManager:
    """Coordinate metric emission across registered backends."""

    def __init__(self, emitters: Iterable[TelemetryEmitter]):
        self._emitters = list(emitters)

    def emit_metric(self, metric: TelemetryMetric) -> None:
        for emitter in self._emitters:
            emitter.emit_metric(metric)

    def emit_usage_event(self, event_type: str, payload: Dict[str, object]) -> bool:
        return any(emitter.emit_usage_event(event_type, payload) for emitter in self._emitters)

    def flush(self) -> None:
        for emitter in self._emitters:
            emitter.flush()
