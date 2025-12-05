from __future__ import annotations

from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION
from cosmos.listeners import dag_run_listener
from cosmos.plugin.airflow3 import CosmosAF3Plugin

# The Cosmos AF3 plugin is only loaded if the Airflow version is greater than 3.0.
if version.parse(airflow_version).major < _AIRFLOW3_MAJOR_VERSION:
    import pytest

    pytest.skip("Skipping AF3 plugin tests on Airflow 2.x", allow_module_level=True)


import importlib
import json
import os
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

skip_pre_airflow_31 = pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("3.1.0"),
    reason="AF3 plugin only supported on Airflow >= 3.1",
)


@pytest.fixture(autouse=True)
def _isolate_env():
    # Clear AIRFLOW__API__BASE_URL for each test using patch.dict
    with patch.dict(os.environ, {"AIRFLOW__API__BASE_URL": ""}, clear=False):
        yield


def _reload_af3_module(api_base: str | None = None):
    # Reload module to recompute API_BASE_PATH under patched env
    with patch.dict(os.environ, {"AIRFLOW__API__BASE_URL": api_base or ""}, clear=False):
        import cosmos.plugin.airflow3 as af3

        importlib.reload(af3)
        return af3


def _reload_af3_with_version(af_ver: str):
    import cosmos.plugin.airflow3 as af3

    with patch.object(af3.airflow, "__version__", af_ver):
        importlib.reload(af3)
        return af3


def _app_with_projects(projects: dict[str, dict[str, str]]):
    af3 = _reload_af3_module()
    with patch("cosmos.plugin.airflow3._load_projects_from_conf", return_value=projects):
        app = af3.create_cosmos_fastapi_app()
    return af3, app


@skip_pre_airflow_31
def test_dbt_docs_view_and_index_local(tmp_path: Path):
    # Arrange: create local docs files
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    index_file = docs_dir / "index.html"
    index_file.write_text("<head></head><body>dbt</body>")
    manifest = docs_dir / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {}}))
    catalog = docs_dir / "catalog.json"
    catalog.write_text(json.dumps({"sources": {}}))

    projects = {"core": {"dir": str(docs_dir), "index": "index.html", "name": "Core"}}
    af3, app = _app_with_projects(projects)

    client = TestClient(app)

    # View page returns iframe pointing to index route
    r = client.get("/core/dbt_docs")
    assert r.status_code == 200
    assert f"/cosmos/core/dbt_docs_index.html" in r.text

    # Index route returns HTML with injected iframe script and CSP header
    r = client.get("/core/dbt_docs_index.html")
    assert r.status_code == 200
    assert "frame-ancestors 'self'" in r.headers.get("Content-Security-Policy", "")
    assert "hashchange" in r.text  # from iframe_script

    # manifest.json and catalog.json return parsed JSON
    r = client.get("/core/manifest.json")
    assert r.status_code == 200
    assert r.json() == {"nodes": {}}

    r = client.get("/core/catalog.json")
    assert r.status_code == 200
    assert r.json() == {"sources": {}}


@skip_pre_airflow_31
def test_index_missing_file_returns_404(tmp_path: Path):
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    # no index.html created
    projects = {"core": {"dir": str(docs_dir), "index": "index.html", "name": "Core"}}
    af3, app = _app_with_projects(projects)
    client = TestClient(app)

    r = client.get("/core/dbt_docs_index.html")
    assert r.status_code == 404
    assert "index not found" in r.text


@skip_pre_airflow_31
def test_manifest_and_catalog_error_500(tmp_path: Path):
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    projects = {"core": {"dir": str(docs_dir), "index": "index.html", "name": "Core"}}
    af3, app = _app_with_projects(projects)
    client = TestClient(app)

    # Patch open_file to raise generic Exception to trigger 500 paths
    with patch("cosmos.plugin.airflow3.open_file", side_effect=RuntimeError("boom")):
        r = client.get("/core/manifest.json")
        assert r.status_code == 500
        assert "manifest read failed" in r.text

        r = client.get("/core/catalog.json")
        assert r.status_code == 500
        assert "catalog read failed" in r.text


@skip_pre_airflow_31
def test_external_view_href_uses_api_base_path():
    # Ensure base path is respected (e.g., Astronomer deployment prefix)
    _reload_af3_module(api_base="https://host/prefix/")
    # Stub projects before constructing plugin
    with patch(
        "cosmos.plugin.airflow3._load_projects_from_conf",
        return_value={"core": {"dir": "/x", "index": "index.html"}},
    ):
        from cosmos.plugin.airflow3 import CosmosAF3Plugin

        plugin = CosmosAF3Plugin()
        assert plugin.external_views
        assert plugin.external_views[0]["href"].startswith("/prefix/")
        assert plugin.external_views[0]["href"].endswith("/cosmos/core/dbt_docs_index.html")


@skip_pre_airflow_31
def test_external_view_href_no_base_path():
    _reload_af3_module(api_base="")

    from cosmos.plugin.airflow3 import CosmosAF3Plugin

    with patch(
        "cosmos.plugin.airflow3._load_projects_from_conf",
        return_value={"core": {"dir": "/x", "index": "index.html"}},
    ):
        plugin = CosmosAF3Plugin()
    assert plugin.external_views[0]["href"].startswith("/cosmos/")


@skip_pre_airflow_31
def test_not_configured_routes():
    af3, app = _app_with_projects({"core": {}})
    client = TestClient(app)

    r = client.get("/core/dbt_docs")
    assert r.status_code == 200
    assert "not configured" in r.text.lower()

    r = client.get("/core/manifest.json")
    assert r.status_code == 404
    assert r.json()["error"] == "not configured"

    r = client.get("/core/dbt_docs_index.html")
    assert r.status_code == 404
    assert "not configured" in r.text.lower()


def test_open_file_remote_uses_objectstorage():
    af3 = _reload_af3_module()

    class _FakeFile:
        def __init__(self, data: str):
            self._data = data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return self._data

    class _FakePath:
        def __init__(self, path: str, conn_id=None):
            self.path = path
            self.conn_id = conn_id

        def open(self, mode="r", **kwargs):
            assert mode == "r"
            return _FakeFile("REMOTE_CONTENT")

    # Avoid touching real connection_env/ObjectStoragePath
    with patch("cosmos.plugin.airflow3.connection_env", side_effect=lambda *_a, **_k: nullcontext()):
        with patch("cosmos.plugin.airflow3.ObjectStoragePath", _FakePath):
            assert af3.open_file("http://example", conn_id="my_conn") == "REMOTE_CONTENT"


def test_open_file_gcs_uses_objectstorage():
    af3 = _reload_af3_module()

    class _FakeFile:
        def __init__(self, data: str):
            self._data = data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return self._data

    class _FakePath:
        def __init__(self, path: str, conn_id=None):
            self.path = path
            self.conn_id = conn_id

        def open(self, mode="r", **kwargs):
            assert mode == "r"
            return _FakeFile("GCS_CONTENT")

    with patch("cosmos.plugin.airflow3.connection_env", side_effect=lambda *_a, **_k: nullcontext()):
        with patch("cosmos.plugin.airflow3.ObjectStoragePath", _FakePath):
            assert af3.open_file("gs://bucket/obj", conn_id=None) == "GCS_CONTENT"


def test_version_gate_raises_on_pre_31():
    af3 = _reload_af3_with_version("3.0.9")
    with pytest.raises(RuntimeError, match="requires Airflow >= 3.1"):
        af3.ensure_airflow_version_supported()
    # Also validate app creation enforces gate
    with pytest.raises(RuntimeError):
        af3.create_cosmos_fastapi_app()


def test_plugin_init_raises_on_pre_31():
    af3 = _reload_af3_with_version("3.0.9")
    with patch("cosmos.plugin.airflow3._load_projects_from_conf", return_value={}):
        with pytest.raises(RuntimeError):
            af3.CosmosAF3Plugin()


@pytest.mark.parametrize(
    "af_ver, should_use_ctx",
    [
        ("3.1.0", True),
        ("3.1.1", False),
        ("3.1.2", False),
        ("3.2.0", False),
    ],
)
def test_connection_env_usage_depends_on_version(af_ver: str, should_use_ctx: bool):
    af3 = _reload_af3_with_version(af_ver)

    import contextlib

    class _FakeFile:
        def __init__(self, data: str):
            self._data = data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return self._data

    class _FakePath:
        def __init__(self, path: str, conn_id=None):
            self.path = path
            self.conn_id = conn_id

        def open(self, mode="r", **kwargs):
            return _FakeFile("OK")

    ctx_calls = {"count": 0}

    @contextlib.contextmanager
    def _ctx(_conn_id=None):  # type: ignore[no-redef]
        ctx_calls["count"] += 1
        yield

    with patch("cosmos.plugin.airflow3.ObjectStoragePath", _FakePath):
        with patch("cosmos.plugin.airflow3.connection_env", _ctx):
            # Call the internal helper directly to isolate behavior
            out = af3._read_content_via_object_storage("gs://x", conn_id="c1")
            assert out == "OK"

    assert (ctx_calls["count"] > 0) == should_use_ctx


def test_load_projects_from_conf_valid_json():
    af3 = _reload_af3_module()

    def fake_get(section, key, fallback=None):
        if section == "cosmos" and key == "dbt_docs_projects":
            return '{"core": {"dir": "/x", "index": "idx.html", "name": "Core"}}'
        return fallback

    with patch.object(af3.conf, "get", side_effect=fake_get):
        projects = af3._load_projects_from_conf()
    assert projects["core"]["dir"] == "/x"
    assert projects["core"]["index"] == "idx.html"
    assert projects["core"]["name"] == "Core"


@skip_pre_airflow_31
def test_index_raises_exception_returns_500(tmp_path: Path):
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    projects = {"core": {"dir": str(docs_dir), "index": "index.html", "name": "Core"}}
    af3, app = _app_with_projects(projects)
    client = TestClient(app)

    with patch("cosmos.plugin.airflow3.open_file", side_effect=RuntimeError("boom")):
        r = client.get("/core/dbt_docs_index.html")
    assert r.status_code == 500
    assert "Cosmos dbt docs error" in r.text


@skip_pre_airflow_31
def test_catalog_not_configured_returns_404():
    af3, app = _app_with_projects({"core": {}})
    client = TestClient(app)
    r = client.get("/core/catalog.json")
    assert r.status_code == 404
    assert r.json()["error"] == "not configured"


@skip_pre_airflow_31
def test_manifest_missing_includes_path_and_connid(tmp_path: Path):
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    # Ensure manifest is missing; create only catalog
    (docs_dir / "catalog.json").write_text(json.dumps({}))
    projects = {
        "core": {
            "dir": str(docs_dir),
            "index": "index.html",
            "name": "Core",
            "conn_id": "my_conn",
        }
    }
    af3, app = _app_with_projects(projects)
    client = TestClient(app)
    r = client.get("/core/manifest.json")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "manifest not found"
    assert body["slug"] == "core"
    assert body["conn_id"] == "my_conn"
    assert body["path"].endswith("/target/manifest.json")


@skip_pre_airflow_31
def test_catalog_missing_includes_path_and_connid(tmp_path: Path):
    docs_dir = tmp_path / "target"
    docs_dir.mkdir(parents=True)
    # Ensure catalog is missing; create only manifest
    (docs_dir / "manifest.json").write_text(json.dumps({}))
    projects = {
        "core": {
            "dir": str(docs_dir),
            "index": "index.html",
            "name": "Core",
            "conn_id": "my_conn",
        }
    }
    af3, app = _app_with_projects(projects)
    client = TestClient(app)
    r = client.get("/core/catalog.json")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "catalog not found"
    assert body["slug"] == "core"
    assert body["conn_id"] == "my_conn"
    assert body["path"].endswith("/target/catalog.json")


def test_dbt_docs_projects_malformed_json_raises(caplog):
    import cosmos.plugin.airflow3 as af3

    importlib.reload(af3)

    def fake_get(section, key, fallback=None):
        if (section, key) == ("cosmos", "dbt_docs_projects"):
            return "{bad json}"
        return fallback

    with patch.object(af3.conf, "get", side_effect=fake_get), caplog.at_level("ERROR"):
        with pytest.raises(json.JSONDecodeError):
            af3._load_projects_from_conf()
        assert "Invalid JSON in [cosmos] dbt_docs_projects:" in caplog.text


@skip_pre_airflow_31
def test_plugin_registers_listeners():
    """Ensure CosmosAF3Plugin registers the listeners."""
    plugin = CosmosAF3Plugin()

    assert hasattr(plugin, "listeners"), "Plugin must define a `listeners` attribute"

    assert dag_run_listener in plugin.listeners, "CosmosAF3Plugin.listeners must include dag_run_listener module"
