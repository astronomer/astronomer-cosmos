from __future__ import annotations

from typing import TYPE_CHECKING

from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION

if TYPE_CHECKING:  # pragma: no cover
    from .airflow2 import CosmosPlugin as _CosmosPluginType
    from .airflow3 import CosmosAF3Plugin as _CosmosAF3PluginType

_CosmosPlugin: _CosmosPluginType | _CosmosAF3PluginType | None = None


def __getattr__(name: str) -> _CosmosPluginType | _CosmosAF3PluginType | None:
    if name == "CosmosPlugin":
        global _CosmosPlugin
        if _CosmosPlugin is None:
            if version.parse(airflow_version).major < _AIRFLOW3_MAJOR_VERSION:
                from .airflow2 import CosmosPlugin  # type: ignore[assignment]  # noqa: F401

                _CosmosPlugin = CosmosPlugin  # type: ignore[assignment]
            else:
                from .airflow3 import CosmosAF3Plugin  # type: ignore[assignment]  # noqa: F401

                _CosmosPlugin = CosmosAF3Plugin  # type: ignore[assignment]
        return _CosmosPlugin
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
