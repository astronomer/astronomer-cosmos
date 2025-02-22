"""Maps Airflow DuckDB connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class DuckDBUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow DuckDB connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup
    https://github.com/astronomer/airflow-provider-duckdb
    """

    airflow_connection_type: str = "duckdb"
    dbt_profile_type: str = "duckdb"

    required_fields = [
        "path"
    ]
    airflow_param_mapping = {
        "path": "path",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            **self.mapped_params,
            **self.profile_args
        }

        if "schema" in self.profile_args:
            profile["schema"] = self.profile_args["schema"]

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile"""
        profile_dict = {
            **super().mock_profile,
        }
        user_defined_schema = self.profile_args.get("schema")
        if user_defined_schema:
            profile_dict["schema"] = user_defined_schema
        return profile_dict
