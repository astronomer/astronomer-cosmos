"Contains a function to get the profile mapping based on the connection ID."

from __future__ import annotations

from typing import Type

from airflow.hooks.base import BaseHook

from .base import BaseProfileMapping, InvalidMappingException
from .bigquery import GoogleCloudServiceAccountFileProfileMapping
from .databricks import DatabricksTokenProfileMapping
from .postgres import PostgresProfileMapping
from .redshift import RedshiftPasswordProfileMapping
from .snowflake import SnowflakeUserPassProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    GoogleCloudServiceAccountFileProfileMapping,
    DatabricksTokenProfileMapping,
    PostgresProfileMapping,
    RedshiftPasswordProfileMapping,
    SnowflakeUserPassProfileMapping,
]

def get_profile_mapping(
    conn_id: str,
    profile_args: dict[str, str] | None = None,
) -> BaseProfileMapping:
    """
    Returns a profile mapping object based on the connection ID.
    """
    if not profile_args:
        profile_args = {}

    # get the connection from Airflow
    hook = BaseHook.get_hook(conn_id)
    conn = hook.get_connection(conn_id)

    if not conn:
        raise ValueError(f"Could not find connection {conn_id}.")

    for profile_mapping in profile_mappings:
        try:
            mapping = profile_mapping(conn, profile_args)
            if mapping.validate_connection():
                return mapping
        except InvalidMappingException:
            continue

    raise ValueError(
        f"Could not find a profile mapping for connection {conn_id}.")
