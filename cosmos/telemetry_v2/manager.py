from __future__ import annotations

from typing import Iterable

from cosmos.telemetry.base import TelemetryEmitter, TelemetryManager
from cosmos.telemetry.scarf_emitter import ScarfTelemetryEmitter
from cosmos.telemetry.statsd import StatsdTelemetryEmitter


def build_default_telemetry_manager(backends: Iterable[str]) -> TelemetryManager:
    emitters: list[TelemetryEmitter] = []
    normalized_backends = {backend.strip().lower() for backend in backends}

    if "statsd" in normalized_backends:
        emitters.append(StatsdTelemetryEmitter())
    if "scarf" in normalized_backends:
        emitters.append(ScarfTelemetryEmitter())

    if not emitters:
        from cosmos.telemetry.base import NullTelemetryEmitter

        emitters.append(NullTelemetryEmitter())

    return TelemetryManager(emitters)
