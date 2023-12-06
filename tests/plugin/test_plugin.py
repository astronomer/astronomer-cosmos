# dbt-core relies on Jinja2>3, whereas Flask<2 relies on an incompatible version of Jinja2.
#
# This discrepancy causes the automated integration tests to fail, as dbt-core is installed in the same
# environment as apache-airflow.
#
# We can get around this by patching the jinja2 namespace to include the deprecated objects:
try:
    import flask  # noqa: F401
except ImportError:
    import markupsafe
    import jinja2

    jinja2.Markup = markupsafe.Markup
    jinja2.escape = markupsafe.escape

from unittest.mock import patch, MagicMock

import sys
import pytest
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.db import initdb, resetdb
from airflow.www.app import cached_app
from airflow.www.extensions.init_appbuilder import AirflowAppBuilder
from flask.testing import FlaskClient

import cosmos.plugin

from cosmos.plugin import (
    dbt_docs_view,
    iframe_script,
    open_gcs_file,
    open_azure_file,
    open_http_file,
    open_s3_file,
    open_file,
)


original_conf_get = conf.get


@pytest.fixture(scope="module")
def app() -> FlaskClient:
    initdb()

    app = cached_app(testing=True)
    appbuilder: AirflowAppBuilder = app.extensions["appbuilder"]

    appbuilder.sm.check_authorization = lambda *args, **kwargs: True

    if dbt_docs_view not in appbuilder.baseviews:
        appbuilder._check_and_init(dbt_docs_view)
        appbuilder.register_blueprint(dbt_docs_view)

    yield app.test_client()

    resetdb(skip_init=True)


@patch.object(cosmos.plugin, "open_file")
def test_dbt_docs(mock_open_file, monkeypatch, app):
    def conf_get(section, key, *args, **kwargs):
        if section == "cosmos" and key == "dbt_docs_dir":
            return "path/to/docs/dir"
        else:
            return original_conf_get(section, key, *args, **kwargs)

    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200


@patch.object(cosmos.plugin, "open_file")
@pytest.mark.parametrize("artifact", ["dbt_docs_index.html", "manifest.json", "catalog.json"])
def test_dbt_docs_artifact(mock_open_file, monkeypatch, app, artifact):
    def conf_get(section, key, *args, **kwargs):
        if section == "cosmos" and key == "dbt_docs_dir":
            return "path/to/docs/dir"
        else:
            return original_conf_get(section, key, *args, **kwargs)

    monkeypatch.setattr(conf, "get", conf_get)

    if artifact == "dbt_docs_index.html":
        mock_open_file.return_value = "<head></head><body></body>"
    else:
        mock_open_file.return_value = "{}"

    response = app.get(f"/cosmos/{artifact}")

    mock_open_file.assert_called_once()
    assert response.status_code == 200
    if artifact == "dbt_docs_index.html":
        # Airflow < 2.4 uses an old version of Werkzeug that does not have Response.text.
        if not hasattr(response, "text"):
            assert iframe_script in response.get_data(as_text=True)
        else:
            assert iframe_script in response.text


@pytest.mark.parametrize("artifact", ["dbt_docs_index.html", "manifest.json", "catalog.json"])
def test_dbt_docs_artifact_missing(app, artifact, monkeypatch):
    def conf_get(section, key, *args, **kwargs):
        if section == "cosmos":
            raise AirflowConfigException
        else:
            return original_conf_get(section, key, *args, **kwargs)

    monkeypatch.setattr(conf, "get", conf_get)

    response = app.get(f"/cosmos/{artifact}")
    assert response.status_code == 404


@pytest.mark.parametrize(
    "path,open_file_callback",
    [
        ("s3://my-bucket/my/path/", "open_s3_file"),
        ("gs://my-bucket/my/path/", "open_gcs_file"),
        ("wasb://my-bucket/my/path/", "open_azure_file"),
        ("http://my-bucket/my/path/", "open_http_file"),
        ("https://my-bucket/my/path/", "open_http_file"),
    ],
)
def test_open_file_calls(path, open_file_callback, monkeypatch):
    def conf_get(section, key, *args, **kwargs):
        if section == "cosmos" and key == "dbt_docs_conn_id":
            return "mock_conn_id"
        else:
            return original_conf_get(section, key, *args, **kwargs)

    monkeypatch.setattr(conf, "get", conf_get)

    with patch.object(cosmos.plugin, open_file_callback) as mock_callback:
        mock_callback.return_value = "mock file contents"
        res = open_file(path)

    mock_callback.assert_called_with(conn_id="mock_conn_id", path=path)
    assert res == "mock file contents"


def test_open_s3_file():
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.amazon.aws.hooks.s3": mock_module}):
        mock_hook = mock_module.S3Hook.return_value = MagicMock()
        mock_hook.read_key.return_value = "mock file contents"

        res = open_s3_file(conn_id="mock_conn_id", path="s3://mock-path/to/docs")

        mock_module.S3Hook.assert_called_once_with(aws_conn_id="mock_conn_id")
        mock_hook.read_key.assert_called_once_with(bucket_name="mock-path", key="to/docs")
        assert res == "mock file contents"


def test_open_gcs_file():
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.google.cloud.hooks.gcs": mock_module}):
        mock_hook = mock_module.GCSHook.return_value = MagicMock()
        mock_hook.download.return_value = b"mock file contents"

        res = open_gcs_file(conn_id="mock_conn_id", path="gs://mock-path/to/docs")

        mock_module.GCSHook.assert_called_once_with(gcp_conn_id="mock_conn_id")
        mock_hook.download.assert_called_once_with(bucket_name="mock-path", object_name="to/docs")
        assert res == "mock file contents"


def test_open_azure_file():
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.microsoft.azure.hooks.wasb": mock_module}):
        mock_hook = mock_module.WasbHook.return_value = MagicMock()
        mock_hook.read_file.return_value = "mock file contents"

        res = open_azure_file(conn_id="mock_conn_id", path="wasb://mock-path/to/docs")

        mock_module.WasbHook.assert_called_once_with(wasb_conn_id="mock_conn_id")
        mock_hook.read_file.assert_called_once_with(container_name="mock-path", blob_name="to/docs")
        assert res == "mock file contents"


def test_open_http_file():
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.http.hooks.http": mock_module}):
        mock_hook = mock_module.HttpHook.return_value = MagicMock()
        mock_response = mock_hook.run.return_value = MagicMock()
        mock_hook.check_response.return_value = mock_response
        mock_response.text = "mock file contents"

        res = open_http_file(conn_id="mock_conn_id", path="http://mock-path/to/docs")

        mock_module.HttpHook.assert_called_once_with(method="GET", http_conn_id="mock_conn_id")
        mock_hook.run.assert_called_once_with(endpoint="http://mock-path/to/docs")
        assert res == "mock file contents"
