"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any
from logging import getLogger

from .base import BaseProfileMapping

logger = getLogger(__name__)


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
        Return a dbt Redshift profile based on the Airflow Redshift connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
        https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
        """
        profile = {
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

        return self.filter_null(profile)

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("password"): str(self.conn.password),
        }
