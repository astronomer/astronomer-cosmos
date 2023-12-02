from unittest.mock import patch

import pytest
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.db import initdb, resetdb
from airflow.www.app import cached_app
from airflow.www.extensions.init_appbuilder import AirflowAppBuilder
from flask.testing import FlaskClient

from cosmos.plugin import dbt_docs_view, iframe_script


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


@patch("cosmos.plugin.open_file")
def test_dbt_docs(mock_open_file, monkeypatch, app):
    def conf_get(section, key, *args, **kwargs):
        if section == "cosmos" and key == "dbt_docs_dir":
            return "path/to/docs/dir"
        else:
            return original_conf_get(section, key, *args, **kwargs)

    response = app.get("/cosmos/dbt_docs")
    assert response.status_code == 200


@patch("cosmos.plugin.open_file")
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
