"""Maps Airflow Oracle connections using user + password authentication to dbt profiles."""

from __future__ import annotations

import re
from typing import Any

from ..base import BaseProfileMapping


class OracleUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Oracle connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/reference/warehouse-setups/oracle-setup
    https://airflow.apache.org/docs/apache-airflow-providers-oracle/stable/connections/oracle.html
    """

    airflow_connection_type: str = "oracle"
    dbt_profile_type: str = "oracle"
    is_community: bool = True

    required_fields = [
        "user",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "port": "port",
        "service": "extra.service_name",
        "user": "login",
        "password": "password",
        "database": "extra.service_name",
        "connection_string": "extra.dsn",
    }

    @property
    def env_vars(self) -> dict[str, str]:
        """Set oracle thick mode."""
        env_vars = super().env_vars
        if self._get_airflow_conn_field("extra.thick_mode"):
            env_vars["ORA_PYTHON_DRIVER_TYPE"] = "thick"
        return env_vars

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            "protocol": "tcp",
            "port": 1521,
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        if "schema" not in profile and "user" in profile:
            proxy = re.search(r"\[([^]]+)\]", profile["user"])
            if proxy:
                profile["schema"] = proxy.group(1)
            else:
                profile["schema"] = profile["user"]
        if "schema" in self.profile_args:
            profile["schema"] = self.profile_args["schema"]

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile. Defaults port to 1521."""
        profile_dict = {
            "protocol": "tcp",
            "port": 1521,
            **super().mock_profile,
        }

        if "schema" not in profile_dict and "user" in profile_dict:
            proxy = re.search(r"\[([^]]+)\]", profile_dict["user"])
            if proxy:
                profile_dict["schema"] = proxy.group(1)
            else:
                profile_dict["schema"] = profile_dict["user"]

        user_defined_schema = self.profile_args.get("schema")
        if user_defined_schema:
            profile_dict["schema"] = user_defined_schema
        return profile_dict
