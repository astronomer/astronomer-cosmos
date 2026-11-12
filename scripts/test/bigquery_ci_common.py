"""Shared utilities for BigQuery CI helper scripts."""

from __future__ import annotations

import json
import os
from typing import Any

from airflow.models.connection import Connection


def keyfile(conn_env_var: str = "AIRFLOW_CONN_GCP_GS_CONN") -> dict[str, Any]:
    """Read the service account key dictionary from an Airflow GCP connection.

    The connection is read from the environment variable specified by ``conn_env_var``
    (defaulting to ``AIRFLOW_CONN_GCP_GS_CONN``). It can be either a JSON string or a URI.
    The key dictionary is extracted from ``keyfile_dict`` or
    ``extra__google_cloud_platform__keyfile_dict`` in the connection's extra field.

    Raises:
        KeyError: If the environment variable is not set.
        SystemExit: If the connection does not contain a service account key dictionary.
    """
    raw = os.environ[conn_env_var]
    conn = Connection.from_json(raw) if raw.lstrip().startswith("{") else Connection(uri=raw)
    extra = conn.extra_dejson
    key = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
    if isinstance(key, str):
        key = json.loads(key)
    if not key:
        raise SystemExit(f"{conn_env_var} has no keyfile_dict")
    return dict(key)


get_keyfile = keyfile
