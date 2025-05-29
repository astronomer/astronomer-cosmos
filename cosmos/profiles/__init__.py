"""Contains a function to get the profile mapping based on the connection ID."""

from __future__ import annotations

from typing import Any, Type

from .athena import AthenaAccessKeyProfileMapping
from .base import BaseProfileMapping, DbtProfileConfigVars
from .bigquery.oauth import GoogleCloudOauthProfileMapping
from .bigquery.service_account_file import GoogleCloudServiceAccountFileProfileMapping
from .bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping
from .clickhouse.user_pass import ClickhouseUserPasswordProfileMapping
from .databricks.oauth import DatabricksOauthProfileMapping
from .databricks.token import DatabricksTokenProfileMapping
from .duckdb.user_pass import DuckDBUserPasswordProfileMapping
from .exasol.user_pass import ExasolUserPasswordProfileMapping
from .oracle.user_pass import OracleUserPasswordProfileMapping
from .postgres.user_pass import PostgresUserPasswordProfileMapping
from .redshift.user_pass import RedshiftUserPasswordProfileMapping
from .snowflake.user_encrypted_privatekey_env_variable import SnowflakeEncryptedPrivateKeyPemProfileMapping
from .snowflake.user_encrypted_privatekey_file import SnowflakeEncryptedPrivateKeyFilePemProfileMapping
from .snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from .snowflake.user_privatekey import SnowflakePrivateKeyPemProfileMapping
from .spark.thrift import SparkThriftProfileMapping
from .sqlserver.standard_sqlserver_auth import StandardSQLServerAuth
from .teradata.user_pass import TeradataUserPasswordProfileMapping
from .trino.certificate import TrinoCertificateProfileMapping
from .trino.jwt import TrinoJWTProfileMapping
from .trino.ldap import TrinoLDAPProfileMapping
from .vertica.user_pass import VerticaUserPasswordProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    AthenaAccessKeyProfileMapping,
    ClickhouseUserPasswordProfileMapping,
    GoogleCloudServiceAccountFileProfileMapping,
    GoogleCloudServiceAccountDictProfileMapping,
    GoogleCloudOauthProfileMapping,
    DatabricksTokenProfileMapping,
    DatabricksOauthProfileMapping,
    DuckDBUserPasswordProfileMapping,
    OracleUserPasswordProfileMapping,
    DuckDBUserPasswordProfileMapping,
    PostgresUserPasswordProfileMapping,
    RedshiftUserPasswordProfileMapping,
    SnowflakeUserPasswordProfileMapping,
    SnowflakeEncryptedPrivateKeyFilePemProfileMapping,
    SnowflakeEncryptedPrivateKeyPemProfileMapping,
    SnowflakePrivateKeyPemProfileMapping,
    SparkThriftProfileMapping,
    ExasolUserPasswordProfileMapping,
    TeradataUserPasswordProfileMapping,
    TrinoLDAPProfileMapping,
    TrinoCertificateProfileMapping,
    TrinoJWTProfileMapping,
    VerticaUserPasswordProfileMapping,
    StandardSQLServerAuth,
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
    "AthenaAccessKeyProfileMapping",
    "BaseProfileMapping",
    "GoogleCloudServiceAccountFileProfileMapping",
    "GoogleCloudServiceAccountDictProfileMapping",
    "GoogleCloudOauthProfileMapping",
    "DatabricksTokenProfileMapping",
    "DatabricksOauthProfileMapping",
    "DbtProfileConfigVars",
    "DuckDBUserPasswordProfileMapping",
    "OracleUserPasswordProfileMapping",
    "PostgresUserPasswordProfileMapping",
    "RedshiftUserPasswordProfileMapping",
    "SnowflakeUserPasswordProfileMapping",
    "SnowflakePrivateKeyPemProfileMapping",
    "SnowflakeEncryptedPrivateKeyFilePemProfileMapping",
    "SparkThriftProfileMapping",
    "ExasolUserPasswordProfileMapping",
    "TeradataUserPasswordProfileMapping",
    "TrinoLDAPProfileMapping",
    "TrinoCertificateProfileMapping",
    "TrinoJWTProfileMapping",
    "VerticaUserPasswordProfileMapping",
    "StandardSQLServerAuth",
]
