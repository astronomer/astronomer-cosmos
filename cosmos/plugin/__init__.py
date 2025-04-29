from __future__ import annotations

from typing import TYPE_CHECKING

from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION

if TYPE_CHECKING:  # pragma: no cover
    from .plugin_impl import CosmosPlugin as _CosmosPluginType

CosmosPlugin: _CosmosPluginType | None = None
# The plugin is only loaded if the Airflow version is less than 3.0. This is because the plugin is incompatible with
# Airflow 3.0 and above. Once the compatibility issue is resolved as part of
# https://github.com/astronomer/astronomer-cosmos/issues/1587, the import statement can be moved outside of the
# conditional block.
if version.parse(airflow_version).major < _AIRFLOW3_MAJOR_VERSION:
    from .plugin_impl import CosmosPlugin as CosmosPlugin  # type: ignore[assignment]
