from __future__ import annotations

import html
import json
import logging
import os
import os.path as op
from contextlib import contextmanager
from typing import Any, Generator, Optional
from unittest.mock import patch
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import ObjectStoragePath
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response

from cosmos.constants import AIRFLOW_OBJECT_STORAGE_PATH_URL_SCHEMES
from cosmos.plugin.snippets import IFRAME_SCRIPT

API_BASE = conf.get("api", "base_url", fallback="")  # reads AIRFLOW__API__BASE_URL
API_BASE_PATH = urlsplit(API_BASE).path.rstrip("/")


# TODO: This context manager and its usage in the method "_read_content_via_object_storage" has been added due
# to the current limitation in Airflow 3.1.0 where plugins are not able to resolve connections via the API server.
# Once this is fixed, potentially in PR https://github.com/apache/airflow/pull/56602 planned to be released in
# Airflow 3.1.1, test the fix and remove this context manager and its usage.
@contextmanager
def connection_env(conn_id: str | None = None) -> Generator[None, None, None]:  # pragma: no cover
    """
    Temporarily expose a connection as AIRFLOW_CONN_{CONN_ID} in the environment.

    This allows hooks and SDK code resolving connections via the environment
    variables backend to find the connection during the scope of the context.
    """
    if conn_id is None:
        yield

    from airflow.models.connection import Connection as ORMConnection

    conn = ORMConnection.get_connection_from_secrets(conn_id)  # type: ignore[arg-type]
    env_name = f"AIRFLOW_CONN_{conn_id.upper()}"  # type: ignore[union-attr]
    env_value = conn.get_uri()
    with patch.dict(os.environ, {env_name: env_value}, clear=False):
        yield


def _read_content_via_object_storage(path: str, conn_id: str | None = None) -> Any:
    with connection_env(conn_id):
        p = ObjectStoragePath(path, conn_id=conn_id) if conn_id else ObjectStoragePath(path)
        with p.open("r") as f:  # type: ignore[no-untyped-call]
            content = f.read()  # type: ignore[no-any-return]
        return content


def open_file(path: str, conn_id: str | None = None) -> Any:
    """
    Retrieve a file from http, https, gs, s3, or wasb.

    Raise a (base Python) FileNotFoundError if the file is not found.
    """
    if path.strip().startswith(AIRFLOW_OBJECT_STORAGE_PATH_URL_SCHEMES):
        return _read_content_via_object_storage(path, conn_id=conn_id)
    else:
        with open(path) as f:
            content = f.read()
        return content  # type: ignore[no-any-return]


def _load_projects_from_conf() -> dict[str, dict[str, Optional[str]]]:
    """
    Load dbt docs projects configuration.

    Supports either:
    - [cosmos] dbt_docs_projects = JSON mapping of project slug to {"dir","conn_id","index"}
    - Legacy single-project settings: dbt_docs_dir, dbt_docs_conn_id, dbt_docs_index_file_name
    """
    projects_raw = conf.get("cosmos", "dbt_docs_projects", fallback=None)
    projects: dict[str, dict[str, Optional[str]]] = {}
    if projects_raw:
        parsed = None
        try:
            parsed = json.loads(projects_raw)
        except json.JSONDecodeError:
            logging.exception("Invalid JSON in [cosmos] dbt_docs_projects: %s", projects_raw)
            raise

        if isinstance(parsed, dict):
            for key, value in parsed.items():
                if not isinstance(value, dict):  # pragma: no cover
                    continue
                projects[str(key)] = {
                    "dir": value.get("dir"),
                    "conn_id": value.get("conn_id"),
                    "index": value.get("index", "index.html"),
                    "name": value.get("name"),
                }

    return projects


def create_cosmos_fastapi_app() -> FastAPI:  # noqa: C901
    app = FastAPI()

    projects = _load_projects_from_conf()

    # Dynamic endpoints for each project
    for slug, cfg in projects.items():
        # Simple HTML wrapper to embed the dbt docs UI
        @app.get(f"/{slug}/dbt_docs", response_class=HTMLResponse)
        def dbt_docs_view(slug_alias: str = slug) -> str:  # type: ignore[no-redef]
            cfg_local = projects.get(slug_alias, {})
            if not cfg_local.get("dir"):
                return "<div>dbt Docs are not configured.</div>"
            iframe_src = f"/cosmos/{slug_alias}/dbt_docs_index.html"
            safe_iframe_src = html.escape(iframe_src, quote=True)
            return (
                '<div style="height:100%;display:flex;flex-direction:column;">'
                f'<iframe src="{safe_iframe_src}" style="border:0;flex:1 1 auto;"></iframe>'
                "</div>"
            )

        # Serve the index with injected iframe script and CSP header
        @app.get(
            f"/{slug}/dbt_docs_index.html",
            response_class=HTMLResponse,
        )
        def dbt_docs_index(slug_alias: str = slug) -> Response:  # type: ignore[no-redef]
            cfg_local = projects.get(slug_alias, {})
            docs_dir_local = cfg_local.get("dir")
            conn_id_local = cfg_local.get("conn_id")
            index_local = cfg_local.get("index") or "index.html"
            if not docs_dir_local:
                return HTMLResponse(
                    content=(
                        f"<pre>Cosmos dbt docs error: slug={html.escape(slug_alias, quote=True)} not configured (missing dir)</pre>"
                    ),
                    status_code=404,
                )
            try:
                html_content = open_file(op.join(docs_dir_local, index_local), conn_id=conn_id_local)
            except FileNotFoundError:
                return HTMLResponse(
                    content=(
                        f"<pre>Cosmos dbt docs error: index not found for slug={html.escape(slug_alias, quote=True)}\n"
                        f"path={html.escape(op.join(docs_dir_local, index_local), quote=True)} conn_id={html.escape(conn_id_local or '', quote=True)}</pre>"
                    ),
                    status_code=404,
                )
            except (OSError, ValueError, RuntimeError, TimeoutError, PermissionError):
                logging.exception(
                    f"Cosmos dbt docs error: index read failed for slug={slug_alias}, path={op.join(docs_dir_local, index_local)}, conn_id={conn_id_local}"
                )
                return HTMLResponse(
                    content=(
                        f"<pre>Cosmos dbt docs error: An internal error occurred loading index for slug={html.escape(slug_alias, quote=True)}</pre>"
                    ),
                    status_code=500,
                )
            html_content = html_content.replace("</head>", f"{IFRAME_SCRIPT}</head>")
            return HTMLResponse(content=html_content, headers={"Content-Security-Policy": "frame-ancestors 'self'"})

        # JSON artifacts
        @app.get(f"/{slug}/manifest.json")
        def manifest(slug_alias: str = slug) -> Response:  # type: ignore[no-redef]
            cfg_local = projects.get(slug_alias, {})
            docs_dir_local = cfg_local.get("dir")
            conn_id_local = cfg_local.get("conn_id")
            if not docs_dir_local:
                return JSONResponse(content={"error": "not configured", "slug": slug_alias}, status_code=404)
            try:
                data = open_file(op.join(docs_dir_local, "manifest.json"), conn_id=conn_id_local)
            except FileNotFoundError:
                return JSONResponse(
                    content={
                        "error": "manifest not found",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "manifest.json"),
                        "conn_id": conn_id_local,
                    },
                    status_code=404,
                )
            except (OSError, ValueError, RuntimeError, TimeoutError, PermissionError) as e:
                logging.exception(
                    f"Error reading manifest for slug '{slug_alias}', path '{op.join(docs_dir_local, 'manifest.json')}', conn_id '{conn_id_local}': {e}"
                )
                return JSONResponse(
                    content={
                        "error": "manifest read failed",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "manifest.json"),
                        "conn_id": conn_id_local,
                    },
                    status_code=500,
                )
            return JSONResponse(content=json.loads(data))

        @app.get(f"/{slug}/catalog.json")
        def catalog(slug_alias: str = slug) -> Response:  # type: ignore[no-redef]
            cfg_local = projects.get(slug_alias, {})
            docs_dir_local = cfg_local.get("dir")
            conn_id_local = cfg_local.get("conn_id")
            if not docs_dir_local:
                return JSONResponse(content={"error": "not configured", "slug": slug_alias}, status_code=404)
            try:
                data = open_file(op.join(docs_dir_local, "catalog.json"), conn_id=conn_id_local)
            except FileNotFoundError:
                return JSONResponse(
                    content={
                        "error": "catalog not found",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "catalog.json"),
                        "conn_id": conn_id_local,
                    },
                    status_code=404,
                )
            except (OSError, ValueError, RuntimeError, TimeoutError, PermissionError) as e:
                logging.exception(
                    f"Error reading catalog for slug '{slug_alias}', path '{op.join(docs_dir_local, 'catalog.json')}', conn_id '{conn_id_local}': {e}"
                )
                return JSONResponse(
                    content={
                        "error": "catalog read failed",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "catalog.json"),
                        "conn_id": conn_id_local,
                    },
                    status_code=500,
                )
            return JSONResponse(content=json.loads(data))

    return app


class CosmosAF3Plugin(AirflowPlugin):
    name = "cosmos"

    # Mount our FastAPI sub-app under /cosmos
    fastapi_apps = [
        {
            "name": "cosmos",
            "app": create_cosmos_fastapi_app(),
            "url_prefix": "/cosmos",
        }
    ]

    # Register external views for navigation
    external_views: list[dict[str, Any]] = []

    def __init__(self) -> None:
        super().__init__()
        projects = _load_projects_from_conf()
        for slug, cfg in projects.items():
            display_name = cfg.get("name") or f"dbt Docs ({slug})"
            self.external_views.append(
                {
                    "name": display_name,
                    "category": "Browse",
                    "href": f"{API_BASE_PATH}/cosmos/{slug}/dbt_docs_index.html",
                }
            )
