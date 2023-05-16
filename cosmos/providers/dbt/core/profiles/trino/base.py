"""
Contains the base Airflow Trino connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class TrinoBaseProfileMapping(BaseProfileMapping):
    """
    Base class responsible for mapping Airflow Trino connections to dbt profiles.
    """

    airflow_connection_type: str = "trino"
    is_community: bool = True

    # https://docs.getdbt.com/reference/warehouse-setups/trino-setup
    required_fields = [
        "host",
        "database",
        "schema",
        "port",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Trino profile based on the Airflow Trino connection. Only handles
        the common fields, the subclasses handle the authentication.
        """
        profile_vars = {
            "type": "trino",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schema": self.schema,
            "session_properties": self.session_properties,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    @property
    def host(self) -> str | None:
        """
        Host can come from:
        - profile_args.host
        - Airflow conn.host

        Also removes http:// or https:// from the host.
        """
        host = self.profile_args.get("host")
        if host:
            return str(host).replace("http://", "").replace("https://", "")

        if self.conn.host:
            return self.conn.host.replace("http://", "").replace("https://", "")

        return None

    @property
    def database(self) -> str | None:
        """
        Database can come from:
        - profile_args.database
        """
        if self.profile_args.get("database"):
            return self.profile_args.get("database")

        return None

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args.schema
        """
        if self.profile_args.get("schema"):
            return self.profile_args.get("schema")

        return None

    @property
    def port(self) -> int | None:
        """
        Port can come from:
        - profile_args.port
        - Airflow conn.port
        """
        port = self.profile_args.get("port")
        if port:
            return int(port)

        if self.conn.port:
            return int(self.conn.port)

        return None

    @property
    def session_properties(self) -> dict[str, Any] | None:
        """
        Session properties can come from:
        - profile_args.session_properties
        - Airflow conn.extra.session_properties
        """
        session_properties = self.profile_args.get("session_properties")
        if session_properties:
            if not isinstance(session_properties, dict):
                raise TypeError(
                    f"session_properties must be a dictionary, got {type(session_properties)}"
                )

            return session_properties

        if self.conn.extra_dejson.get("session_properties"):
            return self.conn.extra_dejson.get("session_properties")

        return None
