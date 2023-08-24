"Maps Airflow Trino connections to LDAP Trino dbt profiles."
from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoLDAPProfileMapping(TrinoBaseProfileMapping):
    """
    Maps Airflow Trino connections to LDAP Trino dbt profiles.

    https://docs.getdbt.com/reference/warehouse-setups/trino-setup#ldap
    https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections.html
    """

    dbt_profile_method: str = "ldap"

    required_fields = TrinoBaseProfileMapping.required_fields + [
        "user",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "user": "login",
        "password": "password",
        **TrinoBaseProfileMapping.airflow_param_mapping,
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """
        Returns a dbt Trino profile based on the Airflow Trino connection.
        """
        common_profile_vars = super().profile
        profile_vars = {
            **self.mapped_params,
            **common_profile_vars,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        # remove any null values
        return self.filter_null(profile_vars)
