"""
Contains the Certificate Airflow Trino connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoCertificateProfileMapping(TrinoBaseProfileMapping):
    """
    Class responsible for mapping Airflow Trino connections to certificate Trino dbt profiles.
    """

    required_fields = TrinoBaseProfileMapping.required_fields + [
        "client_certificate",
        "client_private_key",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Returns a dbt Trino profile based on the Airflow Trino connection.
        """
        common_profile_vars = super().get_profile()
        profile_vars = {
            **common_profile_vars,
            "method": "certificate",
            "client_certificate": self.client_certificate,
            "client_private_key": self.client_private_key,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    @property
    def client_certificate(self) -> str | None:
        """
        client_certificate can come from:
        - profile_args.client_certificate
        - Airflow conn.extra.certs__client_cert_path
        """
        if self.profile_args.get("client_certificate"):
            return str(self.profile_args.get("client_certificate"))

        if self.conn.extra_dejson.get("certs__client_cert_path"):
            return str(self.conn.extra_dejson.get("certs__client_cert_path"))

        return None

    @property
    def client_private_key(self) -> str | None:
        """
        client_private_key can come from:
        - profile_args.client_private_key
        - Airflow conn.extra.certs__client_key_path
        """
        if self.profile_args.get("client_private_key"):
            return str(self.profile_args.get("client_private_key"))

        if self.conn.extra_dejson.get("certs__client_key_path"):
            return str(self.conn.extra_dejson.get("certs__client_key_path"))

        return None
