"""
Contains the Airflow Exasol connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from ..base import BaseProfileMapping

logger = getLogger(__name__)


class ExasolUserPasswordProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Exasol connections to dbt profiles.
    """

    airflow_connection_type: str = "exasol"
    is_community: bool = True

    default_port: int = 8563

    # https://docs.getdbt.com/reference/warehouse-setups/exasol-setup
    required_fields = [
        "threads",
        "dsn",
        "user",
        "password",
        "dbname",
        "schema",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Spark profile based on the Airflow Spark connection.
        """
        profile_vars = {
            "type": "exasol",
            "threads": self.threads,
            "dsn": self.dsn,
            "user": self.user,
            "dbname": self.dbname,
            "schema": self.schema,
            "encryption": self.conn.extra_dejson.get("encryption"),
            "compression": self.conn.extra_dejson.get("compression"),
            "connect_timeout": self.conn.extra_dejson.get("connection_timeout"),
            "socket_timeout": self.conn.extra_dejson.get("socket_timeout"),
            "protocol_version": self.conn.extra_dejson.get("protocol_version"),
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def get_env_vars(self) -> dict[str, str]:
        """
        Return a dictionary of environment variables that should be set.
        """
        env_vars = {}

        # should always be set because we validate in validate_connection
        if self.password:
            env_vars[self.get_env_var_name("password")] = self.password

        return env_vars

    @property
    def threads(self) -> int | None:
        """
        threads can come from:
        - profile_args.threads
        """
        threads = self.profile_args.get("threads")
        if threads:
            return int(threads)

        return None

    @property
    def dsn(self) -> str | None:
        """
        dsn can come from:
        - profile_args.dsn
        - Airflow's conn.host (and conn.port)

        If the connection's host doesn't have a port, we use the default port.
        """
        if self.profile_args.get("dsn"):
            return self.profile_args.get("dsn")

        if self.conn.host:
            # if we have a port in the host, use that
            if ":" in self.conn.host:
                return self.conn.host

            # otherwise, use the port from the connection
            if self.conn.port:
                return f"{self.conn.host}:{self.conn.port}"

            # otherwise, use the default port
            return f"{self.conn.host}:{self.default_port}"

        return None

    @property
    def password(self) -> str | None:
        """
        Password can come from:
        - profile_args.password
        - Airflow's conn.password
        """
        if self.profile_args.get("password"):
            return self.profile_args.get("password")

        if self.conn.password:
            return str(self.conn.password)

        return None

    @property
    def user(self) -> str | None:
        """
        User can come from:
        - profile_args.user
        - Airflow's conn.login
        """
        if self.profile_args.get("user"):
            return self.profile_args.get("user")

        if self.conn.login:
            return str(self.conn.login)

        return None

    @property
    def dbname(self) -> str | None:
        """
        dbname can come from:
        - profile_args.dbname
        - Airflow's conn.schema
        """
        if self.profile_args.get("dbname"):
            return self.profile_args.get("dbname")

        if self.conn.schema:
            return str(self.conn.schema)

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
