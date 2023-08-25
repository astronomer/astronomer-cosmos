"Maps Airflow Snowflake connections to dbt profiles if they use a user/password."
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..base import BaseProfileMapping

if TYPE_CHECKING:
    from airflow.models import Connection


class SnowflakeUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Snowflake connections to dbt profiles if they use a user/password.
    https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """

    airflow_connection_type: str = "snowflake"
    dbt_profile_type: str = "snowflake"

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "password": "password",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
    }

    @property
    def conn(self) -> Connection:
        """
        Snowflake can be odd because the fields used to be stored with keys in the format
        'extra__snowflake__account', but now are stored as 'account'.

        This standardizes the keys to be 'account', 'database', etc.
        """
        conn = super().conn

        conn_dejson = conn.extra_dejson

        if conn_dejson.get("extra__snowflake__account"):
            conn_dejson = {key.replace("extra__snowflake__", ""): value for key, value in conn_dejson.items()}

        conn.extra = json.dumps(conn_dejson)

        return conn

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile."
        profile_vars = {
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def transform_account(self, account: str) -> str:
        "Transform the account to the format <account>.<region> if it's not already."
        region = self.conn.extra_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)
