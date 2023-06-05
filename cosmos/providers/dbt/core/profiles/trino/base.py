"Maps common fields for Airflow Trino connections to dbt profiles."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class TrinoBaseProfileMapping(BaseProfileMapping):
    "Maps common fields for Airflow Trino connections to dbt profiles."

    airflow_connection_type: str = "trino"
    is_community: bool = True

    required_fields = [
        "host",
        "database",
        "schema",
        "port",
    ]

    airflow_param_mapping = {
        "host": "host",
        "port": "port",
        "session_properties": "extra.session_properties",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile."
        profile_vars = {
            "type": "trino",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schema": self.schema,
            "session_properties": self.conn.extra_dejson.get("session_properties"),
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def transform_host(self, host: str) -> str:
        "Replaces http:// or https:// with nothing."
        return host.replace("http://", "").replace("https://", "")
