"""Maps Airflow Mysql connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class StarrocksUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow MySQL connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/starrocks-setup
    """

    airflow_connection_type: str = "mysql"  # StarRocks support mysql protocol
    dbt_profile_type: str = "starrocks"
    is_community: bool = True

    required_fields = [
        "host",
        "username",
        "password",
        "port",
        "schema",
    ]

    secret_fields = [
        "password",
    ]

    airflow_param_mapping = {
        "host": "host",
        "username": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
    }

    @property
    def profile(self) -> dict[str, str | int | None]:
        """
        Generate the dbt profile configuration for StarRocks.

        Returns:
            dict: Profile configuration compatible with dbt-starrocks
        """
        profile = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        if "schema" in self.profile_args:
            profile["schema"] = self.profile_args["schema"]

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile."""
        profile_dict = {
            **super().mock_profile,
            "port": 9030,
        }
        user_defined_schema = self.profile_args.get("schema")
        if user_defined_schema:
            profile_dict["schema"] = user_defined_schema
        return profile_dict
