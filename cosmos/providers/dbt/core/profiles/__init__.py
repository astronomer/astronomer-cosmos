"Contains a function to get the profile mapping based on the connection ID."

from __future__ import annotations

from typing import Any, Type

from airflow.hooks.base import BaseHook

from .base import BaseProfileMapping
from .bigquery.service_account_file import GoogleCloudServiceAccountFileProfileMapping
from .databricks.token import DatabricksTokenProfileMapping
from .exasol.user_pass import ExasolUserPasswordProfileMapping
from .postgres.user_pass import PostgresUserPasswordProfileMapping
from .redshift.user_pass import RedshiftUserPasswordProfileMapping
from .snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from .spark.thrift import SparkThriftProfileMapping
from .trino.certificate import TrinoCertificateProfileMapping
from .trino.jwt import TrinoJWTProfileMapping
from .trino.ldap import TrinoLDAPProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    GoogleCloudServiceAccountFileProfileMapping,
    DatabricksTokenProfileMapping,
    PostgresUserPasswordProfileMapping,
    RedshiftUserPasswordProfileMapping,
    SnowflakeUserPasswordProfileMapping,
    SparkThriftProfileMapping,
    ExasolUserPasswordProfileMapping,
    TrinoLDAPProfileMapping,
    TrinoCertificateProfileMapping,
    TrinoJWTProfileMapping,
]


def get_profile_mapping(
    conn_id: str,
    profile_args: dict[str, Any] | None = None,
) -> BaseProfileMapping:
    """
    Returns a profile mapping object based on the connection ID.
    """
    if not profile_args:
        profile_args = {}

    # get the connection from Airflow
    conn = BaseHook.get_connection(conn_id)

    if not conn:
        raise ValueError(f"Could not find connection {conn_id}.")

    for profile_mapping in profile_mappings:
        mapping = profile_mapping(conn, profile_args)
        if mapping.can_claim_connection():
            return mapping

    raise ValueError(f"Could not find a profile mapping for connection {conn_id}.")
