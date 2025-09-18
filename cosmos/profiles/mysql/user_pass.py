"""Maps Airflow Mysql connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class MysqlUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow MySQL connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/reference/warehouse-setups/mysql-setup
    https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html
    """

    airflow_connection_type: str = "mysql"
    dbt_profile_type: str = "mysql"
    is_community: bool = True

    required_fields = [
        "server",
        "username",
        "password",
        "schema",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "server": "host",
        "username": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
    }

    @property
    def profile(self) -> dict[str, str | int | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            "port": 3306,
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
        """Gets mock profile. Defaults port to 3306."""
        profile_dict = {
            "port": 3306,
            **super().mock_profile,
        }
        user_defined_schema = self.profile_args.get("schema")
        if user_defined_schema:
            profile_dict["schema"] = user_defined_schema
        return profile_dict
