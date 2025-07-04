"""Maps Airflow Snowflake connections to dbt profiles if they use a user/private key."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cosmos.profiles.snowflake.base import SnowflakeBaseProfileMapping

if TYPE_CHECKING:
    from airflow.models import Connection


class SnowflakeEncryptedPrivateKeyPemProfileMapping(SnowflakeBaseProfileMapping):
    """
    Maps Airflow Snowflake connections to dbt profiles if they use a user/private key.
    https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """

    airflow_connection_type: str = "snowflake"
    dbt_profile_type: str = "snowflake"
    is_community: bool = True

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "private_key",
        "private_key_passphrase",
    ]
    secret_fields = [
        "private_key",
        "private_key_passphrase",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
        "private_key": "extra.private_key_content",
        "private_key_passphrase": "password",
    }

    def can_claim_connection(self) -> bool:
        # Make sure this isn't a private key path credential
        result = super().can_claim_connection()
        if result and self.conn.extra_dejson.get("private_key_file") is not None:
            return False
        return result

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

        private_key_content = conn_dejson.get("private_key_content")
        if private_key_content:
            conn_dejson["private_key_content"] = self._decode_private_key_content(private_key_content)

        conn.extra = json.dumps(conn_dejson)

        return conn

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        profile_vars = super().profile
        profile_vars["private_key"] = self.get_env_var_format("private_key")
        profile_vars["private_key_passphrase"] = self.get_env_var_format("private_key_passphrase")
        return self.filter_null(profile_vars)
