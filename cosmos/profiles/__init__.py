"Contains a function to get the profile mapping based on the connection ID."

from __future__ import annotations

from typing import Any, Type


from .base import BaseProfileMapping
from .bigquery.service_account_file import GoogleCloudServiceAccountFileProfileMapping
from .bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping
from .databricks.token import DatabricksTokenProfileMapping
from .exasol.user_pass import ExasolUserPasswordProfileMapping
from .postgres.user_pass import PostgresUserPasswordProfileMapping
from .redshift.user_pass import RedshiftUserPasswordProfileMapping
from .snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from .snowflake.user_privatekey import SnowflakePrivateKeyPemProfileMapping
from .spark.thrift import SparkThriftProfileMapping
from .trino.certificate import TrinoCertificateProfileMapping
from .trino.jwt import TrinoJWTProfileMapping
from .trino.ldap import TrinoLDAPProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    GoogleCloudServiceAccountFileProfileMapping,
    GoogleCloudServiceAccountDictProfileMapping,
    DatabricksTokenProfileMapping,
    PostgresUserPasswordProfileMapping,
    RedshiftUserPasswordProfileMapping,
    SnowflakeUserPasswordProfileMapping,
    SnowflakePrivateKeyPemProfileMapping,
    SparkThriftProfileMapping,
    ExasolUserPasswordProfileMapping,
    TrinoLDAPProfileMapping,
    TrinoCertificateProfileMapping,
    TrinoJWTProfileMapping,
]


def get_automatic_profile_mapping(
    conn_id: str,
    profile_args: dict[str, Any] | None = None,
) -> BaseProfileMapping:
    """
    Returns a profile mapping object based on the connection ID.
    """
    if not profile_args:
        profile_args = {}

    for profile_mapping in profile_mappings:
        mapping = profile_mapping(conn_id, profile_args)
        if mapping.can_claim_connection():
            return mapping

    raise ValueError(f"Could not find a profile mapping for connection {conn_id}.")


__all__ = [
    "BaseProfileMapping",
    "GoogleCloudServiceAccountFileProfileMapping",
    "GoogleCloudServiceAccountDictProfileMapping",
    "DatabricksTokenProfileMapping",
    "PostgresUserPasswordProfileMapping",
    "RedshiftUserPasswordProfileMapping",
    "SnowflakeUserPasswordProfileMapping",
    "SnowflakePrivateKeyPemProfileMapping",
    "SparkThriftProfileMapping",
    "ExasolUserPasswordProfileMapping",
    "TrinoLDAPProfileMapping",
    "TrinoCertificateProfileMapping",
    "TrinoJWTProfileMapping",
]
