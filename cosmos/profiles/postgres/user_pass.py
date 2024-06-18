"""Maps Airflow Postgres connections using user + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class PostgresUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Postgres connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
    https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html
    """

    airflow_connection_type: str = "postgres"
    dbt_profile_type: str = "postgres"

    required_fields = [
        "host",
        "user",
        "password",
        "dbname",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "port": "port",
        "dbname": "schema",
        "keepalives_idle": "extra.keepalives_idle",
        "sslmode": "extra.sslmode",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            "port": 5432,
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
        """Gets mock profile. Defaults port to 5432."""
        profile_dict = {
            "port": 5432,
            **super().mock_profile,
        }
        user_defined_schema = self.profile_args.get("schema")
        if user_defined_schema:
            profile_dict["schema"] = user_defined_schema
        return profile_dict
