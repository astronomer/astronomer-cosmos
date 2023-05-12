"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import BaseProfileMapping


class RedshiftPasswordProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Redshift connections to dbt profiles.
    """

    connection_type: str = "redshift"

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.

        Required by dbt:
        https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
        - host
        - user
        - password
        - port
        - dbname or database
        - schema
        """
        if self.conn.conn_type != self.connection_type:
            return False

        if not self.conn.host:
            return False

        if not self.conn.login:
            return False

        if not self.conn.password:
            return False

        if not self.conn.port:
            return False

        if not self.database:
            return False

        if not self.schema:
            return False

        return True

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Redshift profile based on the Airflow Redshift connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
        https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
        """
        return {
            "type": "redshift",
            "host": self.conn.host,
            "user": self.conn.login,
            "password": self.get_env_var_format("password"),
            "port": self.conn.port or 5432,
            "dbname": self.database,
            "schema": self.schema,
            "connection_timeout": self.conn.extra_dejson.get("timeout"),
            "sslmode": self.conn.extra_dejson.get("sslmode"),
            "region": self.conn.extra_dejson.get("region"),
            **self.profile_args,
        }

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("password"): str(self.conn.password),
        }
