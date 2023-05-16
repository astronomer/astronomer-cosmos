"""
Contains the LDAP Airflow Trino connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoLDAPProfileMapping(TrinoBaseProfileMapping):
    """
    Class responsible for mapping Airflow Trino connections to LDAP Trino dbt profiles.
    """

    required_fields = TrinoBaseProfileMapping.required_fields + [
        "user",
        "password",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Returns a dbt Trino profile based on the Airflow Trino connection.
        """
        common_profile_vars = super().get_profile()
        profile_vars = {
            **common_profile_vars,
            "method": "ldap",
            "user": self.user,
            "password": self.get_env_var_format("password"),
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("password"): str(self.password),
        }

    @property
    def user(self) -> str | None:
        """
        User can come from:
        - profile_args.user
        - Airflow conn.login
        """
        if self.profile_args.get("user"):
            return str(self.profile_args.get("user"))

        if self.conn.login:
            return str(self.conn.login)

        return None

    @property
    def password(self) -> str | None:
        """
        Password can come from:
        - profile_args.password
        - Airflow conn.password
        """
        if self.profile_args.get("password"):
            return str(self.profile_args.get("password"))

        if self.conn.password:
            return str(self.conn.password)

        return None
