"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from ..base import BaseProfileMapping

logger = getLogger(__name__)


class PostgresUserPasswordProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Postgres connections to dbt profiles.
    """

    airflow_connection_type: str = "postgres"

    # https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
    required_fields = [
        "host",
        "user",
        "password",
        "port",
        "dbname",
        "schema",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Postgres profile based on the Airflow Postgres connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
        https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html
        """
        profile = {
            "type": "postgres",
            "host": self.conn.host,
            "user": self.conn.login,
            "password": self.get_env_var_format("password"),
            "port": self.conn.port or 5432,
            "dbname": self.dbname,
            "schema": self.schema,
            "keepalives_idle": self.conn.extra_dejson.get("keepalives_idle"),
            "sslmode": self.conn.extra_dejson.get("sslmode"),
            **self.profile_args,
        }

        return self.filter_null(profile)

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("password"): str(self.conn.password),
        }

    @property
    def host(self) -> str | None:
        """
        Host can come from:
        - profile_args
        - Airflow conn.host
        """
        if self.profile_args.get("host"):
            return str(self.profile_args.get("host"))

        if self.conn.host:
            return str(self.conn.host)

        return None

    @property
    def user(self) -> str | None:
        """
        User can come from:
        - profile_args
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
        - profile_args
        - Airflow conn.password
        """
        if self.profile_args.get("password"):
            return str(self.profile_args.get("password"))

        if self.conn.password:
            return str(self.conn.password)

        return None

    @property
    def port(self) -> int | None:
        """
        Port can come from:
        - profile_args
        - Airflow conn.port
        """
        port = self.profile_args.get("port")
        if port:
            return int(port)

        if self.conn.port:
            return int(self.conn.port)

        return None

    @property
    def dbname(self) -> str | None:
        """
        dbname can come from:
        - profile_args
        - Airflow conn.schema
        """
        if self.profile_args.get("dbname"):
            return str(self.profile_args.get("dbname"))

        if self.conn.schema:
            return str(self.conn.schema)

        return None

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args
        """
        if self.profile_args.get("schema"):
            return str(self.profile_args.get("schema"))

        return None
