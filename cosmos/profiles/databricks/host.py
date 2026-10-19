"""Host normalization shared by the Databricks profile mappings."""

from __future__ import annotations

import re

SCHEME_PREFIX = re.compile(r"^https?://", re.IGNORECASE)


def normalize_host(host: str) -> str:
    """Reduces an Airflow connection host to the bare hostname dbt-databricks expects."""
    return SCHEME_PREFIX.sub("", host.strip()).rstrip("/")
