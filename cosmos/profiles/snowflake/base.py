"Maps Airflow Snowflake connections to dbt profiles if they use a user/password."
from __future__ import annotations

from typing import TYPE_CHECKING

from ..base import BaseProfileMapping

if TYPE_CHECKING:
    pass


class SnowflakeBaseProfileMapping(BaseProfileMapping):
    """
    Common logic between specific Snowflake Profile Mapping classes.
    """

    airflow_connection_type: str = "snowflake"
    dbt_profile_type: str = "snowflake"

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
    ]

    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
    }

    @property
    def openlineage_namespace(self):
        account = self.profile["account"]
        account_parts = account.split(".")
        amended_account = account
        if len(account_parts) == 1:
            account = account_parts[0]
            region, cloud = "us-west-1", "aws"
            amended_account = f"{account}.{region}.{cloud}"
        elif len(account_parts) == 2:
            account, region = account_parts
            cloud = "aws"
            amended_account = f"{account}.{region}.{cloud}"
        return f"snowflake://{amended_account}"

    def transform_account(self, account: str) -> str:
        "Transform the account to the format <account>.<region> if it's not already."
        region = self.conn.extra_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)
