import os.path as op
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access
from airflow.www.views import AirflowBaseView
from flask import abort, url_for
from flask_appbuilder import AppBuilder, expose

from cosmos.listeners import dag_run_listener
from cosmos.settings import dbt_docs_conn_id, dbt_docs_dir, dbt_docs_index_file_name, in_astro_cloud

if in_astro_cloud:
    MENU_ACCESS_PERMISSIONS = [
        (permissions.ACTION_CAN_ACCESS_MENU, "Custom Menu"),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
    ]
else:
    MENU_ACCESS_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
    ]


def bucket_and_key(path: str) -> Tuple[str, str]:
    parsed_url = urlsplit(path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    return bucket, key


def open_s3_file(path: str, conn_id: Optional[str]) -> str:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from botocore.exceptions import ClientError

    if conn_id is None:
        conn_id = S3Hook.default_conn_name

    hook = S3Hook(aws_conn_id=conn_id)
    bucket, key = bucket_and_key(path)
    try:
        content = hook.read_key(key=key, bucket_name=bucket)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code", "") == "NoSuchKey":
            raise FileNotFoundError(f"{path} does not exist")
        raise e
    return content  # type: ignore[no-any-return]


def open_gcs_file(path: str, conn_id: Optional[str]) -> str:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from google.cloud.exceptions import NotFound

    if conn_id is None:
        conn_id = GCSHook.default_conn_name

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

    if conn_id is None:
        conn_id = WasbHook.default_conn_name

    hook = WasbHook(wasb_conn_id=conn_id)

    container, blob = bucket_and_key(path)
    try:
        content = hook.read_file(container_name=container, blob_name=blob)
    except ResourceNotFoundError:
        raise FileNotFoundError(f"{path} does not exist")
    return content  # type: ignore[no-any-return]


def open_http_file(path: str, conn_id: Optional[str]) -> str:
    from airflow.providers.http.hooks.http import HttpHook
    from requests.exceptions import HTTPError

    if conn_id is None:
        conn_id = ""

    hook = HttpHook(method="GET", http_conn_id=conn_id)
    try:
        res = hook.run(endpoint=path)
        hook.check_response(res)
    except HTTPError as e:
        if str(e).startswith("404"):
            raise FileNotFoundError(f"{path} does not exist")
        raise e
    return res.text  # type: ignore[no-any-return]


def open_file(path: str, conn_id: Optional[str] = None) -> str:
    """
    Retrieve a file from http, https, gs, s3, or wasb.

    Raise a (base Python) FileNotFoundError if the file is not found.
    """
    if path.strip().startswith("s3://"):
        return open_s3_file(path, conn_id=conn_id)
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
  function getMaxElement(side, elements_query) {
    var elements = document.querySelectorAll(elements_query)
    var elementsLength = elements.length,
      elVal = 0,
      maxVal = 0,
      Side = capitalizeFirstLetter(side),
      timer = Date.now()

    for (var i = 0; i < elementsLength; i++) {
      elVal =
        elements[i].getBoundingClientRect()[side] +
        getComputedStyleWrapper('margin' + Side, elements[i])
      if (elVal > maxVal) {
        maxVal = elVal
      }
    }

    timer = Date.now() - timer

    chkEventThottle(timer)

    return maxVal
  }
  var throttledTimer = 16
  function chkEventThottle(timer) {
    if (timer > throttledTimer / 2) {
      throttledTimer = 2 * timer
    }
  }
  function capitalizeFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1)
  }
  function getComputedStyleWrapper(prop, el) {
    var retVal = 0
    el = el || document.body // Not testable in phantonJS

    retVal = document.defaultView.getComputedStyle(el, null)
    retVal = null === retVal ? 0 : retVal[prop]

    return parseInt(retVal)
  }
  window.iFrameResizer = {
    heightCalculationMethod: function getHeight() {
      return Math.max(
        // Overview page
        getMaxElement('bottom', 'div.panel.panel-default') + 50,
        // Model page
        getMaxElement('bottom', 'section.section') + 75,
        // Search page
        getMaxElement('bottom', 'div.result-body') + 125
      )
    }
  }

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


class DbtDocsView(AirflowBaseView):  # type: ignore
    default_view = "dbt_docs"
    route_base = "/cosmos"
    template_folder = op.join(op.dirname(__file__), "templates")
    static_folder = op.join(op.dirname(__file__), "static")

    def create_blueprint(
        self, appbuilder: AppBuilder, endpoint: Optional[str] = None, static_folder: Optional[str] = None
    ) -> None:
        # Make sure the static folder is not overwritten, as we want to use it.
        return super().create_blueprint(appbuilder, endpoint=endpoint, static_folder=self.static_folder)  # type: ignore[no-any-return]

    @expose("/dbt_docs")  # type: ignore[misc]
    @has_access(MENU_ACCESS_PERMISSIONS)  # type: ignore[misc]
    def dbt_docs(self) -> str:
        if dbt_docs_dir is None:
            return self.render_template("dbt_docs_not_set_up.html")  # type: ignore[no-any-return,no-untyped-call]
        return self.render_template("dbt_docs.html")  # type: ignore[no-any-return,no-untyped-call]

    @expose("/dbt_docs_index.html")  # type: ignore[misc]
    @has_access(MENU_ACCESS_PERMISSIONS)  # type: ignore[misc]
    def dbt_docs_index(self) -> Tuple[str, int, Dict[str, Any]]:
        if dbt_docs_dir is None:
            abort(404)
        try:
            html = open_file(op.join(dbt_docs_dir, dbt_docs_index_file_name), conn_id=dbt_docs_conn_id)
        except FileNotFoundError:
            abort(404)
        else:
            # Hack the dbt docs to render properly in an iframe
            iframe_resizer_url = url_for(".static", filename="iframeResizer.contentWindow.min.js")
            html = html.replace("</head>", f'{iframe_script}<script src="{iframe_resizer_url}"></script></head>', 1)
            return html, 200, {"Content-Security-Policy": "frame-ancestors 'self'"}

    @expose("/catalog.json")  # type: ignore[misc]
    @has_access(MENU_ACCESS_PERMISSIONS)  # type: ignore[misc]
    def catalog(self) -> Tuple[str, int, Dict[str, Any]]:
        if dbt_docs_dir is None:
            abort(404)
        try:
            data = open_file(op.join(dbt_docs_dir, "catalog.json"), conn_id=dbt_docs_conn_id)
        except FileNotFoundError:
            abort(404)
        else:
            return data, 200, {"Content-Type": "application/json"}

    @expose("/manifest.json")  # type: ignore[misc]
    @has_access(MENU_ACCESS_PERMISSIONS)  # type: ignore[misc]
    def manifest(self) -> Tuple[str, int, Dict[str, Any]]:
        if dbt_docs_dir is None:
            abort(404)
        try:
            data = open_file(op.join(dbt_docs_dir, "manifest.json"), conn_id=dbt_docs_conn_id)
        except FileNotFoundError:
            abort(404)
        else:
            return data, 200, {"Content-Type": "application/json"}


dbt_docs_view = DbtDocsView()


class CosmosPlugin(AirflowPlugin):
    name = "cosmos"
    item = {
        "name": "dbt Docs",
        "category": "Browse",
        "view": dbt_docs_view,
        "href": conf.get("webserver", "base_url") + "/cosmos/dbt_docs",
    }
    appbuilder_views = [item]
    listeners = [dag_run_listener]
