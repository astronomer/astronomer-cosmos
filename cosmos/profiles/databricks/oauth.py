"""Maps Airflow Databricks connections with the client auth to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class DatabricksOauthProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Databricks connections with the client auth to dbt profiles.

    https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
    """

    airflow_connection_type: str = "databricks"
    dbt_profile_type: str = "databricks"

    required_fields = [
        "host",
        "schema",
        "client_secret",
        "client_id",
        "http_path",
    ]

    secret_fields = ["client_secret", "client_id"]

    airflow_param_mapping = {
        "host": "host",
        "schema": "schema",
        "client_id": ["login", "extra.client_id"],
        "client_secret": ["password", "extra.client_secret"],
        "http_path": "extra.http_path",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Generates profile. The client-id and client-secret is stored in an environment variable."""
        return {
            **self.mapped_params,
            **self.profile_args,
            "auth_type": "oauth",
            "client_secret": self.get_env_var_format("client_secret"),
            "client_id": self.get_env_var_format("client_id"),
        }
