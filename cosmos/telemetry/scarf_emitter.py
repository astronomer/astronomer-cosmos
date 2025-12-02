from __future__ import annotations

from typing import Dict

import httpx

from cosmos import constants
from cosmos.telemetry.base import TelemetryEmitter, TelemetryMetric
from cosmos.telemetry.scarf_client import ScarfTelemetryClient
from cosmos.telemetry.utils import build_query_params


class ScarfTelemetryEmitter(TelemetryEmitter):
    """Emit structured telemetry events to the Scarf gateway."""

    def __init__(self, client: ScarfTelemetryClient | None = None) -> None:
        self._client = client or ScarfTelemetryClient()

    def emit_metric(self, metric: TelemetryMetric) -> None:
        # Scarf currently only handles usage events, not raw metrics
        return

    def emit_usage_event(self, event_type: str, payload: Dict[str, object]) -> None:
        params = build_query_params(event_type, payload)
        try:
            self._client.post(params)
        except httpx.HTTPError:
            # Errors are logged in ScarfTelemetryClient; swallow so telemetry doesn't break tasks
            return

    def flush(self) -> None:
        return
