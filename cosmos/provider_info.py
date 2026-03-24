"""
Required provider info for using Airflow config for configuration
"""

from __future__ import annotations

from typing import Any

from cosmos import __version__  # type: ignore[attr-defined]


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "astronomer-cosmos",  # Required
        "name": "Astronomer Cosmos",  # Required
        "description": "Astronomer Cosmos is a library for rendering dbt workflows in Airflow. Contains dags, task groups, and operators.",  # Required
        "versions": [__version__],  # Required
        "config": {
            "cosmos": {
                "description": None,
                "options": {
                    "propagate_logs": {
                        "description": "Enable log propagation from Cosmos custom logger\n",
                        "version_added": "1.3.0a1",
                        "version_deprecated": "1.6.0a1",
                        "deprecation_reason": "`propagate_logs` is no longer necessary as of Cosmos 1.6.0"
                        " because the issue this option was meant to address is no longer an"
                        " issue with Cosmos's new logging approach.",
                        "type": "boolean",
                        "example": None,
                        "default": "True",
                    },
                },
            },
        },
    }
