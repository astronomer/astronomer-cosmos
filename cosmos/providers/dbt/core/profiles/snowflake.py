"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from .base import BaseProfileMapping

from typing import Any, Callable
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

class SnowflakeUserPassProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Snowflake connections to dbt profiles.
    """

    connection_type: str = "snowflake"

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.

        Required by dbt:
        https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#user--password-authentication
        https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#all-configurations
        - user
        - password
        - account
        - database
        - warehouse
        - schema
        """
        if self.conn.conn_type != self.connection_type:
            return False

        if not self.conn.login:
            return False

        if not self.conn.password:
            return False

        if not self.account:
            return False

        if not self.conn_dejson.get("database"):
            return False

        if not self.conn_dejson.get("warehouse"):
            return False

        if not self.conn.schema:
            return False

        return True

    @property
    def conn_dejson(self) -> dict[str, Any]:
        """
        Return the connection's extra_dejson dict. Snowflake can be odd because
        the fields used to be stored with keys in the format 'extra__snowflake__account',
        but now are stored as 'account'.

        This standardizes the keys to be 'account', 'database', etc.
        """
        conn_dejson = self.conn.extra_dejson

        # check if the keys are in the old format
        if conn_dejson.get("extra__snowflake__account"):
            # if so, update the keys to the new format
            conn_dejson = {
                key.replace("extra__snowflake__", ""): value
                for key, value in conn_dejson.items()
            }

        return conn_dejson

    @property
    def account(self) -> str | None:
        """
        Returns the Snowflake account, formatted how dbt expects it.
        """
        account = self.conn_dejson.get("account")

        if not account:
            return None

        # dbt expects the account to be in the format:
        # <account>.<region>
        # https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#account
        # but airflow doesn't necessarily require this, so we need to reconcile
        region = self.conn_dejson.get("region")
        if region and not region in account:
            account = f"{account}.{region}"

        return account

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Snowflake profile based on the Airflow Snowflake connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
        https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
        """
        return {
            "type": "snowflake",
            "account": self.account,
            "user": self.conn.login,
            "password": self.get_env_var_format('password'),
            "role": self.conn_dejson.get("role"),
            "database": self.database,
            "warehouse": self.conn_dejson.get("warehouse"),
            "schema": self.schema,
            **self.profile_args,
        }

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name('password'): str(self.conn.password),
        }

    @property
    def schema(self) -> str:
        """
        In an Airflow Snowflake connection, the schema property is truly the schema.
        """
        return str(self.conn.schema)

    @property
    def database(self) -> str:
        """
        In an Airflow Snowflake connection, the database is stored in the extra_dejson.
        """
        return str(self.conn_dejson.get("database"))
