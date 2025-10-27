from __future__ import annotations

from typing import TYPE_CHECKING

from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION

if TYPE_CHECKING:  # pragma: no cover
    from .airflow2 import CosmosPlugin as _CosmosPluginType
    from .airflow3 import CosmosAF3Plugin as _CosmosAF3PluginType

CosmosPlugin: _CosmosPluginType | _CosmosAF3PluginType | None = None

# Airflow 2.x (FAB/Flask) plugin
if version.parse(airflow_version).major < _AIRFLOW3_MAJOR_VERSION:
    from .airflow2 import CosmosPlugin as CosmosPlugin  # type: ignore[assignment]  # noqa: F401
else:
    # Airflow 3.x (FastAPI) plugin
    from .airflow3 import CosmosAF3Plugin as CosmosPlugin  # type: ignore[assignment]  # noqa: F401
