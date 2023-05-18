"Maps Airflow Trino connections to JWT Trino dbt profiles."
from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoJWTProfileMapping(TrinoBaseProfileMapping):
    """
    Maps Airflow Trino connections to JWT Trino dbt profiles.

    https://docs.getdbt.com/reference/warehouse-setups/trino-setup#jwt
    https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections.html
    """

    required_fields = TrinoBaseProfileMapping.required_fields + [
        "jwt",
    ]
    secret_fields = [
        "jwt",
    ]
    airflow_param_mapping = {
        "jwt": "extra.jwt__token",
        **TrinoBaseProfileMapping.airflow_param_mapping,
    }

    def get_profile(self) -> dict[str, Any | None]:
        "Gets profile."
        common_profile_vars = super().get_profile()

        # need to remove jwt from profile_args because it will be set as an environment variable
        profile_args = self.profile_args.copy()
        profile_args.pop("jwt", None)

        profile_vars = {
            **common_profile_vars,
            "method": "jwt",
            "jwt": self.get_env_var_format("jwt"),
            **profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)
