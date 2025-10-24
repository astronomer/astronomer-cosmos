from __future__ import annotations

import html
import json
import mimetypes
import os
import os.path as op
from contextlib import contextmanager
from typing import Any, Generator, Optional, TypeVar
from unittest.mock import patch
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import ObjectStoragePath
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

S = TypeVar("S")


API_BASE = conf.get("api", "base_url", fallback="")  # reads AIRFLOW__API__BASE_URL
API_BASE_PATH = urlsplit(API_BASE).path.rstrip("/")


@contextmanager
def connection_env(conn_id: str) -> Generator[None, None, None]:
    """
    Temporarily expose a connection as AIRFLOW_CONN_{CONN_ID} in the environment.

    This allows hooks and SDK code resolving connections via the environment
    variables backend to find the connection during the scope of the context.
    """
    from airflow.models.connection import Connection as ORMConnection

    conn = ORMConnection.get_connection_from_secrets(conn_id)
    env_name = f"AIRFLOW_CONN_{conn_id.upper()}"
    env_value = conn.get_uri()
    with patch.dict(os.environ, {env_name: env_value}, clear=False):
        yield


def _read_text_via_object_storage(path: str, conn_id: Optional[str]) -> str:
    with connection_env(conn_id):
        p = ObjectStoragePath(path, conn_id=conn_id) if conn_id else ObjectStoragePath(path)
        with p.open("r") as f:
            content = f.read()  # type: ignore[no-any-return]
        return content


def open_file(path: str, conn_id: Optional[str] = None) -> str:
    """
    Retrieve a file from http, https, gs, s3, or wasb.

    Raise a (base Python) FileNotFoundError if the file is not found.
    """
    if path.strip().startswith(("s3://", "gs://", "gcs://", "wasb://", "abfs://", "az://", "http://", "https://")):
        return _read_text_via_object_storage(path, conn_id=conn_id)
    else:
        with open(path) as f:
            content = f.read()
        return content  # type: ignore[no-any-return]


iframe_script = """
<script>
  // Prevent parent hash changes from sending a message back to the parent.
  // This is necessary for making sure the browser back button works properly.
  let hashChangeLock = true;

  window.addEventListener('hashchange', function () {
    if (!hashChangeLock) {
      window.parent.postMessage(window.location.hash);
    }
    hashChangeLock = false;
  });

  window.addEventListener('message', function (event) {
    let msgData = event.data;
    if (typeof msgData === 'string' && msgData.startsWith('#!')) {
      let updateUrl = new URL(window.location);
      updateUrl.hash = msgData;
      hashChangeLock = true;
      history.replaceState(null, null, updateUrl);
    }
  });
</script>
"""


def _guess_mime(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"


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
        try:
            parsed = json.loads(projects_raw)
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    if not isinstance(value, dict):
                        continue
                    projects[str(key)] = {
                        "dir": value.get("dir"),
                        "conn_id": value.get("conn_id"),
                        "index": value.get("index", "index.html"),
                        "name": value.get("name"),
                    }
        except Exception:
            # Ignore malformed config; fall back to legacy
            projects = {}

    if not projects:
        # Legacy single-project support
        legacy_dir = conf.get("cosmos", "dbt_docs_dir", fallback=None)
        if legacy_dir:
            projects = {
                "default": {
                    "dir": legacy_dir,
                    "conn_id": conf.get("cosmos", "dbt_docs_conn_id", fallback=None),
                    "index": conf.get("cosmos", "dbt_docs_index_file_name", fallback="index.html"),
                    "name": "dbt Docs",
                }
            }

    return projects


def _is_local_path(path: str) -> bool:
    prefixes = ("s3://", "gs://", "wasb://", "http://", "https://")
    return not any(path.strip().startswith(p) for p in prefixes)


def create_cosmos_fastapi_app() -> FastAPI:
    app = FastAPI()

    projects = _load_projects_from_conf()

    # Dynamic endpoints for each project
    for slug, cfg in projects.items():
        docs_dir = cfg.get("dir") or ""
        conn_id = cfg.get("conn_id")
        index_name = cfg.get("index") or "index.html"

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
                    content=(f"<pre>Cosmos dbt docs error: slug={slug_alias} not configured (missing dir)</pre>"),
                    status_code=404,
                )
            try:
                html = open_file(op.join(docs_dir_local, index_local), conn_id=conn_id_local)
            except FileNotFoundError:
                return HTMLResponse(
                    content=(
                        f"<pre>Cosmos dbt docs error: index not found for slug={slug_alias}\n"
                        f"path={op.join(docs_dir_local, index_local)} conn_id={conn_id_local or ''}</pre>"
                    ),
                    status_code=404,
                )
            except Exception as e:
                return HTMLResponse(
                    content=(
                        f"<pre>Cosmos dbt docs error: index read failed for slug={slug_alias}\n"
                        f"path={op.join(docs_dir_local, index_local)} conn_id={conn_id_local or ''}\n"
                        f"exception={type(e).__name__}: {e}</pre>"
                    ),
                    status_code=500,
                )
            html = html.replace("</head>", f"{iframe_script}</head>")
            return HTMLResponse(content=html, headers={"Content-Security-Policy": "frame-ancestors 'self'"})

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
            except Exception as e:
                return JSONResponse(
                    content={
                        "error": "manifest read failed",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "manifest.json"),
                        "conn_id": conn_id_local,
                        "exception": f"{type(e).__name__}: {e}",
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
            except Exception as e:
                return JSONResponse(
                    content={
                        "error": "catalog read failed",
                        "slug": slug_alias,
                        "path": op.join(docs_dir_local, "catalog.json"),
                        "conn_id": conn_id_local,
                        "exception": f"{type(e).__name__}: {e}",
                    },
                    status_code=500,
                )
            return JSONResponse(content=json.loads(data))

        # Static assets: prefer a StaticFiles mount for local paths; otherwise proxy via open_file
        if docs_dir and _is_local_path(docs_dir):
            assets_dir = op.join(docs_dir, "assets")
            if op.isdir(assets_dir):
                app.mount(f"/{slug}/assets", StaticFiles(directory=assets_dir), name=f"{slug}_assets")
        else:

            @app.get(f"/{slug}/assets/{{path:path}}")
            def remote_asset(path: str, slug_alias: str = slug) -> Response:  # type: ignore[no-redef]
                cfg_local = projects.get(slug_alias, {})
                docs_dir_local = cfg_local.get("dir")
                conn_id_local = cfg_local.get("conn_id")
                if not docs_dir_local:
                    return Response(
                        content=f"not configured slug={slug_alias}", status_code=404, media_type="text/plain"
                    )
                full = op.join(docs_dir_local, "assets", path)
                try:
                    data = open_file(full, conn_id=conn_id_local)
                except FileNotFoundError:
                    return Response(
                        content=f"asset not found slug={slug_alias} path={full}",
                        status_code=404,
                        media_type="text/plain",
                    )
                except Exception as e:
                    return Response(
                        content=(
                            f"asset read failed slug={slug_alias} path={full} conn_id={conn_id_local or ''} "
                            f"exception={type(e).__name__}: {e}"
                        ),
                        status_code=500,
                        media_type="text/plain",
                    )
                return Response(content=data, media_type=_guess_mime(full))

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
