"""Maps Airflow Trino connections to Certificate Trino dbt profiles."""

from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoCertificateProfileMapping(TrinoBaseProfileMapping):
    """
    Maps Airflow Trino connections to Certificate Trino dbt profiles.
    https://docs.getdbt.com/reference/warehouse-setups/trino-setup#certificate
    https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections.html
    """

    dbt_profile_method: str = "certificate"

    required_fields = TrinoBaseProfileMapping.base_fields + [
        "client_certificate",
        "client_private_key",
    ]

    airflow_param_mapping = {
        "client_certificate": "extra.certs__client_cert_path",
        "client_private_key": "extra.certs__client_key_path",
        **TrinoBaseProfileMapping.airflow_param_mapping,
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        common_profile_vars = super().profile
        profile_vars = {
            **self.mapped_params,
            **common_profile_vars,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)
