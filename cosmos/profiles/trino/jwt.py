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
        "jwt_token",
    ]
    secret_fields = [
        "jwt_token",
    ]
    airflow_param_mapping = {
        "jwt_token": "extra.jwt__token",
        **TrinoBaseProfileMapping.airflow_param_mapping,
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile."
        common_profile_vars: dict[str, Any] = super().profile

        # need to remove jwt from profile_args because it will be set as an environment variable
        profile_args = self.profile_args.copy()
        profile_args.pop("jwt", None)

        profile_vars = {
            **common_profile_vars,
            "method": "jwt",
            **profile_args,
            # jwt_token should always get set as env var
            "jwt_token": self.get_env_var_format("jwt_token"),
        }

        # remove any null values
        return self.filter_null(profile_vars)
