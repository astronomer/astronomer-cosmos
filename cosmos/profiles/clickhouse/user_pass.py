"""Maps Airflow Clickhouse connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class ClickhouseUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow generic connections using user + password authentication to dbt Clickhouse profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/clickhouse-setup
    """

    airflow_connection_type: str = "generic"
    dbt_profile_type: str = "clickhouse"
    default_port = 9000
    is_community = True

    required_fields = [
        "host",
        "user",
        "schema",
        "clickhouse",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
        "clickhouse": "extra.clickhouse",
    }

    def _set_default_param(self, profile_dict: dict[str, Any]) -> dict[str, Any]:
        if not profile_dict.get("driver"):
            profile_dict["driver"] = "native"

        if not profile_dict.get("port"):
            profile_dict["port"] = self.default_port

        if not profile_dict.get("secure"):
            profile_dict["secure"] = False
        return profile_dict

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile_dict = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        return self.filter_null(self._set_default_param(profile_dict))

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile."""

        profile_dict = {
            **super().mock_profile,
        }

        return self._set_default_param(profile_dict)
