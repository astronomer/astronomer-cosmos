"""Maps Airflow DuckDB connections using local path mapping to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class DuckDBUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow DuckDB connections using local path mapping to dbt profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup
    https://github.com/astronomer/airflow-provider-duckdb
    """

    airflow_connection_type: str = "duckdb"
    dbt_profile_type: str = "duckdb"
    is_community: bool = True

    # Without this path variable, its required to find the right local db connection
    required_fields = ["path"]

    airflow_param_mapping = {"path": "host"}

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {**self.mapped_params, **self.profile_args}
        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile"""
        profile_dict = {
            **super().mock_profile,
        }
        return profile_dict
