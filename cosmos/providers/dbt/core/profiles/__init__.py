from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional, Type

from .base import BaseProfileMapping
from .postgres import PostgresProfileMapping
from .snowflake import SnowflakeUserPassProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    PostgresProfileMapping,
    SnowflakeUserPassProfileMapping,
]

def get_profile_mapping(
    conn_id: str,
    profile_args: dict[str, str] = {},
) -> BaseProfileMapping:
    """
    Returns a profile mapping object based on the connection ID.
    """
    # get the connection from Airflow
    from airflow.hooks.base import BaseHook
    hook = BaseHook.get_hook(conn_id)
    conn = hook.get_connection(conn_id)

    if not conn:
        raise ValueError(f"Could not find connection {conn_id}.")

    for profile_mapping in profile_mappings:
        mapping = profile_mapping(conn, profile_args)
        if mapping.validate_connection():
            return mapping

    raise ValueError(
        f"Could not find a profile mapping for connection {conn_id}.")
