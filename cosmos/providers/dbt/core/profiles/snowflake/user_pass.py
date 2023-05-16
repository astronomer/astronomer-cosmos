"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class SnowflakeUserPasswordProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Snowflake connections to dbt profiles.
    """

    airflow_connection_type: str = "snowflake"

    # https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#user--password-authentication
    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "password",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Snowflake profile based on the Airflow Snowflake connection.

        Password is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
        https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
        """
        profile_vars = {
            "type": "snowflake",
            "account": self.account,
            "user": self.conn.login,
            "password": self.get_env_var_format("password"),
            "role": self.conn_dejson.get("role"),
            "database": self.database,
            "warehouse": self.conn_dejson.get("warehouse"),
            "schema": self.schema,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("password"): str(self.conn.password),
        }

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
        Snowflake account can come from:
        - profile_args.account
        - Airflow conn.extra.account (and conn.extra.region)

        dbt also expects the account to be in the format <account>.<region> but Airflow
        doesn't necessarily require this, so this also reconciles the account and region.
        https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#account
        """
        if self.profile_args.get("account"):
            return str(self.profile_args.get("account"))

        account = self.conn_dejson.get("account")

        if not account:
            return None

        region = self.conn_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)

    @property
    def user(self) -> str | None:
        """
        User can come from:
        - profile_args.user
        - Airflow conn.login
        """
        if self.profile_args.get("user"):
            return str(self.profile_args.get("user"))

        if self.conn.login:
            return str(self.conn.login)

        return None

    @property
    def database(self) -> str | None:
        """
        Database can come from:
        - profile_args.database
        - Airflow conn.extra.database
        """
        if self.profile_args.get("database"):
            return str(self.profile_args.get("database"))

        if self.conn_dejson.get("database"):
            return str(self.conn_dejson.get("database"))

        return None

    @property
    def warehouse(self) -> str | None:
        """
        Warehouse can come from:
        - profile_args.warehouse
        - Airflow conn.extra.warehouse
        """
        if self.profile_args.get("warehouse"):
            return str(self.profile_args.get("warehouse"))

        if self.conn_dejson.get("warehouse"):
            return str(self.conn_dejson.get("warehouse"))

        return None

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args.schema
        - Airflow conn.schema
        """
        if self.profile_args.get("schema"):
            return str(self.profile_args.get("schema"))

        if self.conn.schema:
            return str(self.conn.schema)

        return None

    @property
    def password(self) -> str | None:
        """
        Password can come from:
        - profile_args.password
        - Airflow conn.password
        """
        if self.profile_args.get("password"):
            return str(self.profile_args.get("password"))

        if self.conn.password:
            return str(self.conn.password)

        return None
