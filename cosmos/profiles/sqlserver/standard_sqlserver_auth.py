"""Maps Airflow Sqlserver connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from cosmos.profiles import BaseProfileMapping


class StandardSQLServerAuth(BaseProfileMapping):

    airflow_connection_type: str = "generic"
    dbt_profile_type: str = "sqlserver"
    default_port = 1433
    is_community = True

    required_fields = [
        "server",
        "user",
        "schema",
        "database",
        "driver",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "server": "host",
        "user": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
        "database": "extra.database",
        "driver": "extra.driver",
    }

    def _set_default_param(self, profile_dict: dict[str, Any]) -> dict[str, Any]:

        if not profile_dict.get("port"):
            profile_dict["port"] = self.default_port

        return profile_dict

    @property
    def profile(self) -> dict[str, Any]:
        profile_dict = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        return self.filter_null(self._set_default_param(profile_dict))

    @property
    def mock_profile(self) -> dict[str, Any]:
        """Gets mock profile."""

        profile_dict = {
            **super().mock_profile,
        }

        return self._set_default_param(profile_dict)
