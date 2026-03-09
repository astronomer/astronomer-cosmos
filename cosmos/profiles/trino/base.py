"""Maps common fields for Airflow Trino connections to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class TrinoBaseProfileMapping(BaseProfileMapping):
    """Maps common fields for Airflow Trino connections to dbt profiles."""

    airflow_connection_type: str = "trino"
    dbt_profile_type: str = "trino"
    is_community: bool = True

    base_fields = [
        "host",
        "database",
        "schema",
        "port",
    ]

    required_fields = base_fields + ["user"]

    airflow_param_mapping = {
        "host": "host",
        "port": "port",
        "user": "login",
        "session_properties": "extra.session_properties",
    }

    @property
    def profile(self) -> dict[str, Any]:
        """Gets profile."""
        profile_vars = {
            **self.mapped_params,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    @property
    def mock_profile(self) -> dict[str, Any]:
        mock_profile = super().mock_profile
        mock_profile["port"] = 99999
        return mock_profile

    def transform_host(self, host: str) -> str:
        """Replaces http:// or https:// with nothing."""
        return host.replace("http://", "").replace("https://", "")
