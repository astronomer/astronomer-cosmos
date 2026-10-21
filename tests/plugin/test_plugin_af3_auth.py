from __future__ import annotations

import pytest
from airflow import __version__ as airflow_version
from packaging.version import Version

if Version(airflow_version) < Version("3.1.0"):
    pytest.skip("The Cosmos AF3 plugin requires Airflow >= 3.1", allow_module_level=True)

import json
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from fastapi.testclient import TestClient
from jwt import InvalidTokenError

VALID_TOKEN = "valid-token"
DBT_DOCS_ROUTES = ("/core/dbt_docs", "/core/dbt_docs_index.html", "/core/manifest.json", "/core/catalog.json")
SECRET_MODEL_SQL = "select credit_card_number from customers"

skip_airflow_without_jwt_cookie_support = pytest.mark.skipif(
    Version(airflow_version) < Version("3.1.1"),
    reason="Airflow only reads the JWT cookie from 3.1.1",
)


@pytest.fixture
def docs_dir(tmp_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    (target / "index.html").write_text("<head></head><body>dbt docs</body>")
    (target / "manifest.json").write_text(json.dumps({"nodes": {"model.a": {"raw_code": SECRET_MODEL_SQL}}}))
    (target / "catalog.json").write_text(json.dumps({"sources": {}}))
    return target


@pytest.fixture
def auth_manager():
    async def get_user_from_token(token: str) -> MagicMock:
        if token != VALID_TOKEN:
            raise InvalidTokenError("invalid token")
        return MagicMock(name="user")

    manager = MagicMock()
    manager.get_user_from_token.side_effect = get_user_from_token
    manager.is_authorized_view.return_value = True
    with patch("airflow.api_fastapi.core_api.security.get_auth_manager", return_value=manager):
        yield manager


@pytest.fixture
def client(docs_dir: Path) -> TestClient:
    import cosmos.plugin.airflow3 as af3

    projects = {"core": {"dir": str(docs_dir), "index": "index.html"}}
    with patch.object(af3, "_load_projects_from_conf", return_value=projects):
        return TestClient(af3.create_cosmos_fastapi_app())


def test_unauthenticated_requests_are_rejected_without_docs_content(client: TestClient, auth_manager: MagicMock):
    for route in DBT_DOCS_ROUTES:
        response = client.get(route)
        assert response.status_code == 401, route
        assert SECRET_MODEL_SQL not in response.text


def test_bearer_token_grants_access(client: TestClient, auth_manager: MagicMock):
    headers = {"Authorization": f"Bearer {VALID_TOKEN}"}

    for route in DBT_DOCS_ROUTES:
        assert client.get(route, headers=headers).status_code == 200, route
    assert SECRET_MODEL_SQL in client.get("/core/manifest.json", headers=headers).text


@skip_airflow_without_jwt_cookie_support
def test_jwt_cookie_grants_access_as_sent_by_the_ui(client: TestClient, auth_manager: MagicMock):
    client.cookies.set(COOKIE_NAME_JWT_TOKEN, VALID_TOKEN)

    for route in DBT_DOCS_ROUTES:
        assert client.get(route).status_code == 200, route


def test_access_is_checked_against_the_website_view(client: TestClient, auth_manager: MagicMock):
    client.get("/core/manifest.json", headers={"Authorization": f"Bearer {VALID_TOKEN}"})

    assert auth_manager.is_authorized_view.call_args.kwargs["access_view"] == AccessView.WEBSITE


def test_user_without_website_access_is_forbidden(client: TestClient, auth_manager: MagicMock):
    auth_manager.is_authorized_view.return_value = False

    response = client.get("/core/manifest.json", headers={"Authorization": f"Bearer {VALID_TOKEN}"})

    assert response.status_code == 403
    assert SECRET_MODEL_SQL not in response.text


def test_invalid_token_is_rejected(client: TestClient, auth_manager: MagicMock):
    response = client.get("/core/manifest.json", headers={"Authorization": "Bearer stale-token"})

    assert response.status_code in (401, 403)
    assert SECRET_MODEL_SQL not in response.text


def test_routes_are_denied_when_airflow_auth_helpers_are_missing(docs_dir: Path, auth_manager: MagicMock):
    import cosmos.plugin.airflow3 as af3

    with patch.dict(sys.modules, {"airflow.api_fastapi.core_api.security": None}):
        dependencies = af3.airflow3_auth.dbt_docs_auth_dependencies()

    with (
        patch.object(af3.airflow3_auth, "dbt_docs_auth_dependencies", return_value=dependencies),
        patch.object(af3, "_load_projects_from_conf", return_value={"core": {"dir": str(docs_dir)}}),
    ):
        client = TestClient(af3.create_cosmos_fastapi_app())

    response = client.get("/core/manifest.json", headers={"Authorization": f"Bearer {VALID_TOKEN}"})
    assert response.status_code == 403
    assert SECRET_MODEL_SQL not in response.text


def test_plugin_still_registers_the_dbt_docs_menu_entry(docs_dir: Path):
    import cosmos.plugin.airflow3 as af3

    projects = {"core": {"dir": str(docs_dir), "index": "index.html", "name": "Core dbt Docs"}}
    with patch.object(af3, "_load_projects_from_conf", return_value=projects):
        plugin = af3.CosmosAF3Plugin()

    assert plugin.external_views == [
        {"name": "Core dbt Docs", "category": "Browse", "href": "/cosmos/core/dbt_docs_index.html"}
    ]
    assert [app["url_prefix"] for app in plugin.fastapi_apps] == ["/cosmos"]


@pytest.mark.parametrize("running_airflow_version, warns", [("3.1.0", True), ("3.1.1", False)])
def test_plugin_warns_that_airflow_310_cannot_open_dbt_docs(
    docs_dir: Path, caplog: pytest.LogCaptureFixture, running_airflow_version: str, warns: bool
):
    import cosmos.plugin.airflow3 as af3

    with (
        patch.object(af3, "AIRFLOW_VERSION", Version(running_airflow_version)),
        patch.object(af3, "_load_projects_from_conf", return_value={"core": {"dir": str(docs_dir)}}),
        caplog.at_level(logging.WARNING),
    ):
        af3.CosmosAF3Plugin()

    assert ("upgrade to Airflow >= 3.1.1" in caplog.text) is warns
