from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION

# The Cosmos plugin is only loaded if the Airflow version is less than 3.0. This is because the plugin is incompatible
# with Airflow 3.0 and above. Once the compatibility issue is resolved as part of
# https://github.com/astronomer/astronomer-cosmos/issues/1587, we can remove this check and run the tests on
# Airflow 3.0+.
if version.parse(airflow_version).major >= _AIRFLOW3_MAJOR_VERSION:
    import pytest

    pytest.skip("Skipping plugin tests on Airflow 3.0+", allow_module_level=True)


# dbt-core relies on Jinja2>3, whereas Flask<2 relies on an incompatible version of Jinja2.
#
# This discrepancy causes the automated integration tests to fail, as dbt-core is installed in the same
# environment as apache-airflow.
#
# We can get around this by patching the jinja2 namespace to include the deprecated objects:
try:
    import flask  # noqa: F401
except ImportError:
    import jinja2
    import markupsafe

    jinja2.Markup = markupsafe.Markup
    jinja2.escape = markupsafe.escape

import importlib
import sys
from importlib.util import find_spec
from unittest.mock import MagicMock, PropertyMock, mock_open, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from airflow.utils.db import initdb, resetdb
from airflow.www.app import cached_app
from airflow.www.extensions.init_appbuilder import AirflowAppBuilder
from flask.testing import FlaskClient

import cosmos
import cosmos.plugin
import cosmos.settings
from cosmos.plugin.airflow2 import (
    IFRAME_SCRIPT,
    open_azure_file,
    open_file,
    open_gcs_file,
    open_http_file,
    open_s3_file,
)


@pytest.fixture(scope="module")
def module_monkeypatch():
    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def app_within_astro_cloud(module_monkeypatch) -> FlaskClient:
    module_monkeypatch.setenv("ASTRONOMER_ENVIRONMENT", "cloud")
    importlib.reload(cosmos.settings)
    importlib.reload(cosmos.plugin.airflow2)
    importlib.reload(cosmos)
    initdb()

    cached_app._cached_app = None
    app = cached_app(testing=True)
    appbuilder: AirflowAppBuilder = app.extensions["appbuilder"]

    appbuilder.sm.check_authorization = lambda *args, **kwargs: True

    if cosmos.plugin.airflow2.dbt_docs_view not in appbuilder.baseviews:
        # unregister blueprints registered in global context
        del app.blueprints["DbtDocsView"]
        keys_to_delete = [view_name for view_name in app.view_functions.keys() if view_name.startswith("DbtDocsView")]
        [app.view_functions.pop(view_name) for view_name in keys_to_delete]

        appbuilder._check_and_init(cosmos.plugin.airflow2.dbt_docs_view)
        appbuilder.register_blueprint(cosmos.plugin.airflow2.dbt_docs_view)

    yield app.test_client()

    resetdb()


@pytest.fixture(scope="module")
def app() -> FlaskClient:
    initdb()

    app = cached_app(testing=True)
    appbuilder: AirflowAppBuilder = app.extensions["appbuilder"]

    appbuilder.sm.check_authorization = lambda *args, **kwargs: True

    if cosmos.plugin.airflow2.dbt_docs_view not in appbuilder.baseviews:
        appbuilder._check_and_init(cosmos.plugin.airflow2.dbt_docs_view)
        appbuilder.register_blueprint(cosmos.plugin.airflow2.dbt_docs_view)

    yield app.test_client()

    resetdb()


@pytest.mark.integration
def test_dbt_docs(monkeypatch, app):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", "path/to/docs/dir")

    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200
    assert "<iframe" in response.text


@pytest.mark.integration
@patch.object(cosmos.plugin.airflow2, "open_file")
def test_dbt_docs_not_set_up(monkeypatch, app):
    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200
    assert "<iframe" not in response.text


@pytest.mark.integration
@patch.object(cosmos.plugin.airflow2, "open_file")
@pytest.mark.parametrize("artifact", ["dbt_docs_index.html", "manifest.json", "catalog.json"])
def test_dbt_docs_artifact(mock_open_file, monkeypatch, app, artifact):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", "path/to/docs/dir")
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_conn_id", "mock_conn_id")
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_index_file_name", "custom_index.html")

    if artifact == "dbt_docs_index.html":
        mock_open_file.return_value = "<head></head><body></body>"
        storage_path = "path/to/docs/dir/custom_index.html"
    else:
        mock_open_file.return_value = "{}"
        storage_path = f"path/to/docs/dir/{artifact}"

    response = app.get(f"/cosmos/{artifact}")

    mock_open_file.assert_called_once_with(storage_path, conn_id="mock_conn_id")
    assert response.status_code == 200
    if artifact == "dbt_docs_index.html":
        assert IFRAME_SCRIPT in response.text
        assert "Content-Security-Policy" in response.headers
        assert response.headers["Content-Security-Policy"] == "frame-ancestors 'self'"


@pytest.mark.integration
@patch.object(cosmos.plugin.airflow2, "open_file")
@pytest.mark.parametrize("artifact", ["dbt_docs_index.html", "manifest.json", "catalog.json"])
def test_dbt_docs_artifact_not_found(mock_open_file, monkeypatch, app, artifact):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", "path/to/docs/dir")
    mock_open_file.side_effect = FileNotFoundError

    response = app.get(f"/cosmos/{artifact}")

    mock_open_file.assert_called_once()
    assert response.status_code == 404


@pytest.mark.integration
@pytest.mark.parametrize("artifact", ["dbt_docs_index.html", "manifest.json", "catalog.json"])
def test_dbt_docs_artifact_missing(app, artifact):
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
def test_open_file_calls(path, open_file_callback):
    with patch.object(cosmos.plugin.airflow2, open_file_callback) as mock_callback:
        mock_callback.return_value = "mock file contents"
        res = open_file(path, conn_id="mock_conn_id")

    mock_callback.assert_called_with(path, conn_id="mock_conn_id")
    assert res == "mock file contents"


@pytest.mark.parametrize("conn_id", ["mock_conn_id", None])
def test_open_s3_file(conn_id):
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.amazon.aws.hooks.s3": mock_module}):
        mock_hook = mock_module.S3Hook.return_value
        mock_hook.read_key.return_value = "mock file contents"

        res = open_s3_file("s3://mock-path/to/docs", conn_id=conn_id)

        if conn_id is not None:
            mock_module.S3Hook.assert_called_once_with(aws_conn_id=conn_id)

        mock_hook.read_key.assert_called_once_with(bucket_name="mock-path", key="to/docs")
        assert res == "mock file contents"


@pytest.mark.skipif(
    find_spec("airflow.providers.google") is None,
    reason="apache-airflow-providers-amazon not installed, which is required for this test.",
)
def test_open_s3_file_not_found():
    from botocore.exceptions import ClientError

    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.amazon.aws.hooks.s3": mock_module}):
        mock_hook = mock_module.S3Hook.return_value

        def side_effect(*args, **kwargs):
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "")

        mock_hook.read_key.side_effect = side_effect

        with pytest.raises(FileNotFoundError):
            open_s3_file("s3://mock-path/to/docs", conn_id="mock-conn-id")

        mock_module.S3Hook.assert_called_once()


@pytest.mark.parametrize("conn_id", ["mock_conn_id", None])
def test_open_gcs_file(conn_id):
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.google.cloud.hooks.gcs": mock_module}):
        mock_hook = mock_module.GCSHook.return_value = MagicMock()
        mock_hook.download.return_value = b"mock file contents"

        res = open_gcs_file("gs://mock-path/to/docs", conn_id=conn_id)

        if conn_id is not None:
            mock_module.GCSHook.assert_called_once_with(gcp_conn_id=conn_id)

        mock_hook.download.assert_called_once_with(bucket_name="mock-path", object_name="to/docs")
        assert res == "mock file contents"


@pytest.mark.skipif(
    find_spec("airflow.providers.google") is None,
    reason="apache-airflow-providers-google not installed, which is required for this test.",
)
def test_open_gcs_file_not_found():
    from google.cloud.exceptions import NotFound

    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.google.cloud.hooks.gcs": mock_module}):
        mock_hook = mock_module.GCSHook.return_value = MagicMock()

        def side_effect(*args, **kwargs):
            raise NotFound("")

        mock_hook.download.side_effect = side_effect

        with pytest.raises(FileNotFoundError):
            open_gcs_file("gs://mock-path/to/docs", conn_id="mock-conn-id")

        mock_module.GCSHook.assert_called_once()


@pytest.mark.parametrize("conn_id", ["mock_conn_id", None])
def test_open_azure_file(conn_id):
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.microsoft.azure.hooks.wasb": mock_module}):
        mock_hook = mock_module.WasbHook.return_value = MagicMock()
        mock_hook.default_conn_name = PropertyMock(return_value="default_conn")
        mock_hook.read_file.return_value = "mock file contents"

        res = open_azure_file("wasb://mock-path/to/docs", conn_id=conn_id)

        if conn_id is not None:
            mock_module.WasbHook.assert_called_once_with(wasb_conn_id=conn_id)

        mock_hook.read_file.assert_called_once_with(container_name="mock-path", blob_name="to/docs")
        assert res == "mock file contents"


@pytest.mark.skipif(
    find_spec("airflow.providers.microsoft") is None,
    reason="apache-airflow-providers-microsoft not installed, which is required for this test.",
)
def test_open_azure_file_not_found():
    from azure.core.exceptions import ResourceNotFoundError

    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.microsoft.azure.hooks.wasb": mock_module}):
        mock_hook = mock_module.WasbHook.return_value = MagicMock()

        mock_hook.read_file.side_effect = ResourceNotFoundError

        with pytest.raises(FileNotFoundError):
            open_azure_file("wasb://mock-path/to/docs", conn_id="mock-conn-id")

        mock_module.WasbHook.assert_called_once()


@pytest.mark.parametrize("conn_id", ["mock_conn_id", None])
def test_open_http_file(conn_id):
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.http.hooks.http": mock_module}):
        mock_hook = mock_module.HttpHook.return_value = MagicMock()
        mock_response = mock_hook.run.return_value = MagicMock()
        mock_hook.default_conn_name = PropertyMock(return_value="default_conn")
        mock_hook.check_response.return_value = mock_response
        mock_response.text = "mock file contents"

        res = open_http_file("http://mock-path/to/docs", conn_id=conn_id)

        if conn_id is not None:
            mock_module.HttpHook.assert_called_once_with(method="GET", http_conn_id=conn_id)
        else:
            mock_module.HttpHook.assert_called_once_with(method="GET", http_conn_id="")

        mock_hook.run.assert_called_once_with(endpoint="http://mock-path/to/docs")
        assert res == "mock file contents"


def test_open_http_file_not_found():
    from requests.exceptions import HTTPError

    mock_module = MagicMock()
    with patch.dict(sys.modules, {"airflow.providers.http.hooks.http": mock_module}):
        mock_hook = mock_module.HttpHook.return_value = MagicMock()

        def side_effect(*args, **kwargs):
            raise HTTPError("404 Client Error: Not Found for url: https://google.com/this/is/a/fake/path")

        mock_hook.run.side_effect = side_effect

        with pytest.raises(FileNotFoundError):
            open_http_file("https://google.com/this/is/a/fake/path", conn_id="mock-conn-id")

        mock_module.HttpHook.assert_called_once()


"404 Client Error: Not Found for url: https://google.com/ashjdfasdkfahdjsf"


@patch("builtins.open", new_callable=mock_open, read_data="mock file contents")
def test_open_file_local(mock_file):
    res = open_file("/my/path")
    mock_file.assert_called_with("/my/path")
    assert res == "mock file contents"


@pytest.mark.integration
@pytest.mark.parametrize(
    "url_path", ["/cosmos/dbt_docs", "/cosmos/dbt_docs_index.html", "/cosmos/catalog.json", "/cosmos/manifest.json"]
)
def test_has_access_with_permissions_outside_astro_does_not_include_custom_menu(url_path, app):
    cosmos.plugin.airflow2.dbt_docs_view.appbuilder.sm.check_authorization = MagicMock()
    mock_check_auth = cosmos.plugin.airflow2.dbt_docs_view.appbuilder.sm.check_authorization
    app.get(url_path)
    assert mock_check_auth.call_args[0][0] == [("can_read", "Website")]


@pytest.mark.integration
@pytest.mark.parametrize(
    "url_path", ["/cosmos/dbt_docs", "/cosmos/dbt_docs_index.html", "/cosmos/catalog.json", "/cosmos/manifest.json"]
)
def test_has_access_with_permissions_in_astro_must_include_custom_menu(url_path, app_within_astro_cloud):
    app = app_within_astro_cloud
    cosmos.plugin.airflow2.dbt_docs_view.appbuilder.sm.check_authorization = MagicMock()
    mock_check_auth = cosmos.plugin.airflow2.dbt_docs_view.appbuilder.sm.check_authorization
    app.get(url_path)
    assert mock_check_auth.call_args[0][0] == [("menu_access", "Custom Menu"), ("can_read", "Website")]


def test_cosmos_plugin_enabled_on_airflow2():
    assert cosmos.plugin.CosmosPlugin is not None


@pytest.mark.integration
@patch("cosmos.telemetry.emit_usage_metrics_if_enabled")
def test_dbt_docs_emits_telemetry(mock_emit, monkeypatch, app):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", "s3://my-bucket/docs")
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_conn_id", "my_s3_conn")

    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200
    mock_emit.assert_called_once_with(
        event_type="dbt_docs_access",
        additional_metrics={
            "storage_type": "s3",
            "is_configured": True,
            "uses_custom_conn": True,
        },
    )


@pytest.mark.integration
@patch("cosmos.telemetry.emit_usage_metrics_if_enabled")
def test_dbt_docs_emits_telemetry_not_configured(mock_emit, monkeypatch, app):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", None)
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_conn_id", None)

    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200
    mock_emit.assert_called_once_with(
        event_type="dbt_docs_access",
        additional_metrics={
            "storage_type": "not_configured",
            "is_configured": False,
            "uses_custom_conn": False,
        },
    )


@pytest.mark.integration
@patch("cosmos.telemetry.emit_usage_metrics_if_enabled")
def test_dbt_docs_emits_telemetry_local_storage(mock_emit, monkeypatch, app):
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_dir", "/local/path/to/docs")
    monkeypatch.setattr("cosmos.plugin.airflow2.dbt_docs_conn_id", None)

    response = app.get("/cosmos/dbt_docs")

    assert response.status_code == 200
    mock_emit.assert_called_once_with(
        event_type="dbt_docs_access",
        additional_metrics={
            "storage_type": "local",
            "is_configured": True,
            "uses_custom_conn": False,
        },
    )


@pytest.mark.parametrize(
    "path,expected_type",
    [
        ("s3://bucket/path", "s3"),
        ("gs://bucket/path", "gcs"),
        ("wasb://container/path", "azure"),
        ("http://example.com/path", "http"),
        ("https://example.com/path", "http"),
        ("/local/path", "local"),
    ],
)
def test_get_storage_type(path, expected_type):
    from cosmos.plugin.airflow2 import DbtDocsView

    view = DbtDocsView()
    assert view._get_storage_type(path) == expected_type
