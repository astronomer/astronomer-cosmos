"""Maps Airflow Snowflake connections to dbt profiles if they use a user/non-encrypted private key path."""

from __future__ import annotations

from typing import Any

from cosmos.log import get_logger
from cosmos.profiles.snowflake.base import SnowflakeBaseProfileMapping

logger = get_logger(__name__)


class SnowflakePrivateKeyFilePemProfileMapping(SnowflakeBaseProfileMapping):
    """
    Maps Airflow Snowflake connections to dbt profiles if they use a user/private key path without a passphrase.
    https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """

    airflow_connection_type: str = "snowflake"
    dbt_profile_type: str = "snowflake"
    is_community: bool = False

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "private_key_path",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
        "private_key_path": "extra.private_key_file",
        "query_tag": "extra.query_tag",
    }

    def can_claim_connection(self) -> bool:
        result = super().can_claim_connection()
        if not result:
            return False
        # A passphrase is set: the connection is for an encrypted key. This mapping does not
        # forward the passphrase to dbt, so claiming would yield a profile that fails at runtime.
        # Refuse to claim and surface a warning so the user can switch to
        # SnowflakeEncryptedPrivateKeyFilePemProfileMapping.
        if self.conn.password:
            logger.warning(
                "%s will not claim connection %s: a passphrase is set on the Airflow connection. "
                "Use SnowflakeEncryptedPrivateKeyFilePemProfileMapping for passphrase-protected keys.",
                self.__class__.__name__,
                self.conn_id,
            )
            return False
        return True

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        profile_vars = super().profile
        return self.filter_null(profile_vars)
