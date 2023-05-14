"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import BaseProfileMapping

from logging import getLogger

logger = getLogger(__name__)


class PostgresProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Postgres connections to dbt profiles.
    """

    connection_type: str = "postgres"

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.

        Required by dbt:
        https://docs.getdbt.com/reference/warehouse-setups/postgres-setup
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
            logger.info(
                "Not using mapping %s because host is not set",
                self.__class__.__name__
            )
            return False

        if not self.conn.login:
            logger.info(
                "Not using mapping %s because login is not set",
                self.__class__.__name__
            )
            return False

        if not self.conn.password:
            logger.info(
                "Not using mapping %s because password is not set",
                self.__class__.__name__
            )
            return False

        if not self.conn.port:
            logger.info(
                "Not using mapping %s because port is not set",
                self.__class__.__name__
            )
            return False

        if not self.database:
            logger.info(
                "Not using mapping %s because database is not set",
                self.__class__.__name__
            )
            return False

        if not self.schema:
            logger.info(
                "Not using mapping %s because schema is not set",
                self.__class__.__name__
            )
            return False

        return True

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
            "dbname": self.database,
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
