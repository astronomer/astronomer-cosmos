"Maps Airflow Postgres connections using user + password authentication to dbt profiles."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class ClickhouseUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow generic connections using user + password authentication to dbt Clickhouse profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/clickhouse-setup
    """

    airflow_connection_type: str = "generic"
    default_port = 9000

    required_fields = [
        "host",
        "login",
        "schema",
        "clickhouse",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "login": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
        "clickhouse": "extra.clickhouse",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile. The password is stored in an environment variable."
        profile = {
            "type": "clickhouse",
            "schema": self.conn.schema,
            "login": self.conn.login,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
            "driver": self.conn.extra_dejson.get("driver") or "native",
            "port": self.conn.port or self.default_port,
            "host": self.conn.host,
            "secure": self.conn.extra_dejson.get("secure") or False,
            "keepalives_idle": self.conn.extra_dejson.get("keepalives_idle"),
            "sslmode": self.conn.extra_dejson.get("sslmode"),
            "clickhouse": self.conn.extra_dejson.get("clickhouse"),
            **self.profile_args,
        }

        return self.filter_null(profile)