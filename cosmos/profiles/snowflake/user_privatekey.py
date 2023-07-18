"Maps Airflow Snowflake connections to dbt profiles if they use a user/private key."
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..base import BaseProfileMapping

if TYPE_CHECKING:
    from airflow.models import Connection


class SnowflakePrivateKeyPemProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Snowflake connections to dbt profiles if they use a user/private key.
    https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """

    airflow_connection_type: str = "snowflake"

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "private_key",
    ]
    secret_fields = [
        "private_key",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "private_key": "extra.private_key_content",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
    }

    def __init__(self, conn: Connection, profile_args: dict[str, Any | None] | None = None) -> None:
        """
        Snowflake can be odd because the fields used to be stored with keys in the format
        'extra__snowflake__account', but now are stored as 'account'.

        This standardizes the keys to be 'account', 'database', etc.
        """
        conn_dejson = conn.extra_dejson

        if conn_dejson.get("extra__snowflake__account"):
            conn_dejson = {key.replace("extra__snowflake__", ""): value for key, value in conn_dejson.items()}

        conn.extra = json.dumps(conn_dejson)

        self.conn = conn
        self.profile_args = profile_args or {}
        super().__init__(conn, profile_args)

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile."
        profile_vars = {
            "type": "snowflake",
            "account": self.account,
            "user": self.user,
            "schema": self.schema,
            "database": self.database,
            "role": self.conn.extra_dejson.get("role"),
            "warehouse": self.conn.extra_dejson.get("warehouse"),
            **self.profile_args,
            # private_key should always get set as env var
            "private_key": self.get_env_var_format("private_key"),
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def transform_account(self, account: str) -> str:
        "Transform the account to the format <account>.<region> if it's not already."
        region = self.conn.extra_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)
