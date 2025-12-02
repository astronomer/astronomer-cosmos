"""Helper utilities for telemetry emitters."""
from __future__ import annotations

from typing import Dict


def build_query_params(event_type: str, payload: Dict[str, object]) -> Dict[str, object]:
    """Convert a telemetry payload into the query parameters Scarf expects."""

    params: Dict[str, object] = {
        "event_type": event_type,
    }

    for key, value in payload.items():
        if value is None:
            continue

        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                params[f"{key}_{nested_key}"] = nested_value
        else:
            params[key] = value

    return params

