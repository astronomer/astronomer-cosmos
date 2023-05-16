"""
Contains the JWT Airflow Trino connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import TrinoBaseProfileMapping


class TrinoJWTProfileMapping(TrinoBaseProfileMapping):
    """
    Class responsible for mapping Airflow Trino connections to JWT Trino dbt profiles.
    """

    required_fields = TrinoBaseProfileMapping.required_fields + [
        "jwt",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Returns a dbt Trino profile based on the Airflow Trino connection.
        """
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

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("jwt"): str(self.jwt),
        }

    @property
    def jwt(self) -> str | None:
        """
        jwt can come from:
        - profile_args.jwt
        - Airflow conn.extra.jwt__token
        """
        if self.profile_args.get("jwt"):
            return str(self.profile_args.get("jwt"))

        if self.conn.extra_dejson.get("jwt__token"):
            return str(self.conn.extra_dejson.get("jwt__token"))

        return None
