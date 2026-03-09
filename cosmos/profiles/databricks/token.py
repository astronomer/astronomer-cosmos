"""Maps Airflow Databricks connections with a token to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class DatabricksTokenProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Databricks connections with a token to dbt profiles.

    https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
    """

    airflow_connection_type: str = "databricks"
    dbt_profile_type: str = "databricks"

    required_fields = [
        "host",
        "schema",
        "token",
        "http_path",
    ]

    secret_fields = [
        "token",
    ]

    airflow_param_mapping = {
        "host": "host",
        "schema": "schema",
        "token": ["password", "extra.token"],
        "http_path": "extra.http_path",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Generates profile. The token is stored in an environment variable."""
        return {
            **self.mapped_params,
            **self.profile_args,
            # token should always get set as env var
            "token": self.get_env_var_format("token"),
        }

    def transform_host(self, host: str) -> str:
        """Removes the https:// prefix."""
        return host.replace("https://", "")
