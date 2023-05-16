"""
Contains the Airflow Redshift connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from ..base import BaseProfileMapping

logger = getLogger(__name__)


class RedshiftUserPasswordProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Redshift connections to dbt profiles.
    """

    airflow_connection_type: str = "redshift"
    default_port = 5432

    # https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
    required_fields = [
        "host",
        "user",
        "password",
        "dbname",
        "schema",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Redshift profile based on the Airflow Redshift connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
        https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
        """
        profile = {
            "type": "redshift",
            "host": self.host,
            "user": self.user,
            "password": self.get_env_var_format("password"),
            "port": self.port or 5432,
            "dbname": self.dbname,
            "schema": self.schema,
            "connection_timeout": self.conn.extra_dejson.get("timeout"),
            "sslmode": self.conn.extra_dejson.get("sslmode"),
            "region": self.conn.extra_dejson.get("region"),
            **self.profile_args,
        }

        return self.filter_null(profile)

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        env_vars = {}

        if self.password:
            env_vars[self.get_env_var_name("password")] = self.password

        return env_vars

    @property
    def host(self) -> str | None:
        """
        Host can come from:
        - profile_args.host
        - Airflow conn.host
        """
        if self.profile_args.get("host"):
            return str(self.profile_args["host"])

        if self.conn.host:
            return self.conn.host

        return None

    @property
    def user(self) -> str | None:
        """
        User can come from:
        - profile_args.user
        - Airflow conn.login
        """
        if self.profile_args.get("user"):
            return str(self.profile_args["user"])

        if self.conn.login:
            return self.conn.login

        return None

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args.schema
        """
        if self.profile_args.get("schema"):
            return str(self.profile_args["schema"])

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
            return self.conn.port

        return self.default_port

    @property
    def dbname(self) -> str | None:
        """
        Dbname can come from:
        - profile_args.dbname
        - Airflow conn.schema
        """
        if self.profile_args.get("dbname"):
            return str(self.profile_args["dbname"])

        if self.conn.schema:
            return self.conn.schema

        return None

    @property
    def password(self) -> str | None:
        """
        Password can come from:
        - profile_args.password
        - Airflow conn.password
        """
        if self.profile_args.get("password"):
            return str(self.profile_args["password"])

        if self.conn.password:
            return str(self.conn.password)

        return None
