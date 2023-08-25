"Maps Airflow Exasol connections with a username and password to dbt profiles."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class ExasolUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Exasol connections with a username and password to dbt profiles.
    https://docs.getdbt.com/reference/warehouse-setups/exasol-setup
    """

    airflow_connection_type: str = "exasol"
    dbt_profile_type: str = "exasol"
    is_community: bool = True

    default_port: int = 8563

    required_fields = [
        "threads",
        "dsn",
        "user",
        "password",
        "dbname",
        "schema",
    ]

    secret_fields = [
        "password",
    ]

    airflow_param_mapping = {
        "dsn": "host",
        "user": "login",
        "password": "password",
        "dbname": "schema",
        "encryption": "extra.encryption",
        "compression": "extra.compression",
        "connection_timeout": "extra.connection_timeout",
        "socket_timeout": "extra.socket_timeout",
        "protocol_version": "extra.protocol_version",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile. The password is stored in an environment variable."
        profile_vars = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def transform_dsn(self, host: str) -> str:
        "Adds the port if it's not already there."
        if ":" not in host:
            port = self.conn.port or self.default_port
            return f"{host}:{port}"

        return host
