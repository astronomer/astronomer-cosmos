"""Maps Airflow Snowflake connections to dbt profiles if they use a user/private key."""

from __future__ import annotations

import base64
import binascii
import json
import logging
from typing import TYPE_CHECKING, Any

from cosmos.profiles.snowflake.base import SnowflakeBaseProfileMapping

if TYPE_CHECKING:
    from airflow.models import Connection

logger = logging.getLogger(__name__)


class SnowflakePrivateKeyPemProfileMapping(SnowflakeBaseProfileMapping):
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
    ]
    secret_fields = [
        "private_key",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
        "private_key": "extra.private_key_content",
    }

    def _decode_private_key_content(self, private_key_content: str) -> str:
        """
        Decodes the private key content from either base64-encoded or plain-text PEM format.

        Starting from `apache-airflow-providers-snowflake` version 6.3.0, the provider expects the
        `private_key_content` to be base64-encoded rather than raw PEM text. This method ensures
        compatibility by attempting to decode the content from base64 first. If decoding fails,
        the original content is assumed to be plain-text PEM (as used in older versions).

        This allows backward compatibility while supporting the new expected format.

        Args:
            private_key_content: The private key content, either base64 encoded or plain-text PEM

        Returns:
            The decoded private key in plain-text PEM format
        """
        try:
            decoded_key = base64.b64decode(private_key_content).decode("utf-8")
            return decoded_key
        except (UnicodeDecodeError, binascii.Error):
            return private_key_content

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

        conn_dejson["private_key_content"] = self._decode_private_key_content(conn_dejson["private_key_content"])
        conn.extra = json.dumps(conn_dejson)

        return conn

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        profile_vars = super().profile
        # private_key should always get set as env var
        profile_vars["private_key"] = self.get_env_var_format("private_key")
        return self.filter_null(profile_vars)
