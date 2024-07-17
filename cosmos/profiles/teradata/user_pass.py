"""Maps Airflow Snowflake connections to dbt profiles if they use a user/password."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class TeradataUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Teradata connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/teradata-setup
    https://airflow.apache.org/docs/apache-airflow-providers-teradata/stable/connections/teradata.html
    """

    airflow_connection_type: str = "teradata"
    dbt_profile_type: str = "teradata"
    is_community = True

    required_fields = [
        "host",
        "user",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "schema": "schema",
        "tmode": "extra.tmode",
    }

    @property
    def profile(self) -> dict[str, Any]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }
        # schema is not mandatory in teradata. In teradata user itself a database so if schema is not mentioned
        # in both airflow connection and profile_args then treating user as schema.
        if "schema" not in self.profile_args and self.mapped_params.get("schema") is None:
            profile["schema"] = profile["user"]

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile. Assigning user to schema as default"""
        mock_profile = super().mock_profile
        mock_profile["schema"] = mock_profile["user"]
        return mock_profile
