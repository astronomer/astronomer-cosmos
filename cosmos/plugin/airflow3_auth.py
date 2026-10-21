from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException, status

from cosmos.log import get_logger

logger = get_logger(__name__)


def _forbid() -> None:
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")


def dbt_docs_auth_dependencies() -> list[Any]:
    """
    Return the FastAPI dependencies that protect every Cosmos dbt docs route.

    Airflow 3 attaches authentication per router, so a plugin sub-app mounted under ``/cosmos`` inherits none.
    Access requires the same ``Website`` read permission the Airflow 2 plugin checked, and is denied outright
    if Airflow's auth helpers cannot be imported.
    """
    try:
        from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
        from airflow.api_fastapi.core_api.security import requires_access_view
    except ImportError:
        logger.warning("Airflow API auth helpers are unavailable: denying access to the Cosmos dbt docs routes.")
        return [Depends(_forbid)]

    return [Depends(requires_access_view(AccessView.WEBSITE))]
