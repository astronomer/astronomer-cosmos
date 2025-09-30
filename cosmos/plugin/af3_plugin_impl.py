from __future__ import annotations

import json
import mimetypes
import os.path as op
from typing import Any, Optional, Tuple

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles


def bucket_and_key(path: str) -> Tuple[str, str]:
    from urllib.parse import urlsplit

    parsed_url = urlsplit(path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    return bucket, key


def open_s3_file(path: str, conn_id: Optional[str]) -> str:
    """Read file from S3.

    If conn_id is provided, use Airflow's S3Hook (connection-managed). Otherwise, fall back to boto3
    default credential chain (env/instance profile), avoiding SDK-time Connection loading in web requests.
    """
    bucket, key = bucket_and_key(path)

    if conn_id:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from botocore.exceptions import ClientError

        hook = S3Hook(aws_conn_id=conn_id)
        try:
            content = hook.read_key(key=key, bucket_name=bucket)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") == "NoSuchKey":
                raise FileNotFoundError(f"{path} does not exist")
            raise e
        return content  # type: ignore[no-any-return]

    # boto3 fallback path
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)  # type: ignore[no-any-return]
    except NoCredentialsError as e:
        raise RuntimeError("S3 credentials not found; set AWS_* env or provide conn_id") from e
    except ClientError as e:
        if e.response.get("Error", {}).get("Code", "") in {"NoSuchKey", "404"}:
            raise FileNotFoundError(f"{path} does not exist")
        raise


def open_gcs_file(path: str, conn_id: Optional[str]) -> str:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from google.cloud.exceptions import NotFound

    hook = GCSHook(gcp_conn_id=conn_id)
    bucket, blob = bucket_and_key(path)
    try:
        content = hook.download(bucket_name=bucket, object_name=blob)
    except NotFound:
        raise FileNotFoundError(f"{path} does not exist")
    return content.decode("utf-8")  # type: ignore[no-any-return]


def open_azure_file(path: str, conn_id: Optional[str]) -> str:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from azure.core.exceptions import ResourceNotFoundError

    hook = WasbHook(wasb_conn_id=conn_id)

    container, blob = bucket_and_key(path)
    try:
        content = hook.read_file(container_name=container, blob_name=blob)
    except ResourceNotFoundError:
        raise FileNotFoundError(f"{path} does not exist")
    return content  # type: ignore[no-any-return]


def open_http_file(path: str, conn_id: Optional[str]) -> str:
    if conn_id:
        from airflow.providers.http.hooks.http import HttpHook
        from requests.exceptions import HTTPError

        hook = HttpHook(method="GET", http_conn_id=conn_id)
        try:
            res = hook.run(endpoint=path)
            hook.check_response(res)
        except HTTPError as e:
            if str(e).startswith("404"):
                raise FileNotFoundError(f"{path} does not exist")
            raise e
        return res.text  # type: ignore[no-any-return]
    else:
        import requests

        resp = requests.get(path, timeout=30)
        if resp.status_code == 404:
            raise FileNotFoundError(f"{path} does not exist")
        resp.raise_for_status()
        return resp.text


def open_file(path: str, conn_id: Optional[str] = None) -> str:
    """
    Retrieve a file from http, https, gs, s3, or wasb.

    Raise a (base Python) FileNotFoundError if the file is not found.
    """
    if path.strip().startswith("s3://"):
        # If no conn_id is provided, rely on boto3 environment/instance role credentials
        return open_s3_file(path, conn_id=conn_id if conn_id else None)
    elif path.strip().startswith("gs://"):
        return open_gcs_file(path, conn_id=conn_id)
    elif path.strip().startswith("wasb://"):
        return open_azure_file(path, conn_id=conn_id)
    elif path.strip().startswith("http://") or path.strip().startswith("https://"):
        return open_http_file(path, conn_id=conn_id)
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
            docs_dir_local = cfg_local.get("dir") or ""
            index_local = cfg_local.get("index") or "index.html"
            conn_id_local = cfg_local.get("conn_id") or ""
            mode = "local" if _is_local_path(docs_dir_local) else "remote"
            debug_panel = (
                f'<div style="padding:8px;margin-bottom:6px;background:#fff7cc;border:1px solid #f0d000;">'
                f"<strong>Cosmos dbt docs debug</strong>: slug=<code>{slug_alias}</code>, "
                f"dir=<code>{docs_dir_local}</code>, index=<code>{index_local}</code>, mode=<code>{mode}</code>, "
                f"conn_id=<code>{conn_id_local}</code>. "
                f'Index URL: <a href="{iframe_src}">{iframe_src}</a>. '
                f'Try <a href="/cosmos/{slug_alias}/manifest.json">manifest.json</a> and '
                f'<a href="/cosmos/{slug_alias}/catalog.json">catalog.json</a>.'
                f"</div>"
            )
            return (
                '<div style="height:100%;display:flex;flex-direction:column;">'
                f"{debug_panel}"
                f'<iframe src="{iframe_src}" style="border:0;flex:1 1 auto;"></iframe>'
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
                    "href": f"/cosmos/{slug}/dbt_docs",
                }
            )
