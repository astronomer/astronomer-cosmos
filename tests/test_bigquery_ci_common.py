from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import pytest
import yaml

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts/test"

COMMON_SCRIPT = SCRIPTS_DIR / "bigquery_ci_common.py"
spec_common = importlib.util.spec_from_file_location("bigquery_ci_common", COMMON_SCRIPT)
bigquery_ci_common = importlib.util.module_from_spec(spec_common)
spec_common.loader.exec_module(bigquery_ci_common)

KEYFILE_SCRIPT = SCRIPTS_DIR / "bigquery_ci_keyfile.py"
spec_keyfile = importlib.util.spec_from_file_location("bigquery_ci_keyfile", KEYFILE_SCRIPT)
bigquery_ci_keyfile = importlib.util.module_from_spec(spec_keyfile)
spec_keyfile.loader.exec_module(bigquery_ci_keyfile)

PROFILES_SCRIPT = SCRIPTS_DIR / "bigquery_ci_profiles.py"
spec_profiles = importlib.util.spec_from_file_location("bigquery_ci_profiles", PROFILES_SCRIPT)
bigquery_ci_profiles = importlib.util.module_from_spec(spec_profiles)
spec_profiles.loader.exec_module(bigquery_ci_profiles)

SAMPLE_KEY = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key-id-12345",
    "private_key": "fake-ci-private-key-line-1\nfake-ci-private-key-line-2\n",
    "client_email": "sa@test-project.iam.gserviceaccount.com",
}


def test_keyfile_from_json_dict(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": SAMPLE_KEY}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_from_json_nested_string(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": json.dumps(SAMPLE_KEY)}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_from_json_gcp_prefixed_key(monkeypatch):
    conn_json = json.dumps(
        {
            "conn_type": "google_cloud_platform",
            "extra": {"extra__google_cloud_platform__keyfile_dict": SAMPLE_KEY},
        }
    )
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_from_json_gcp_prefixed_string(monkeypatch):
    conn_json = json.dumps(
        {
            "conn_type": "google_cloud_platform",
            "extra": {"extra__google_cloud_platform__keyfile_dict": json.dumps(SAMPLE_KEY)},
        }
    )
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_from_uri_dict(monkeypatch):
    uri = f"google-cloud-platform:///?__extra__={quote(json.dumps({'keyfile_dict': SAMPLE_KEY}))}"
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", uri)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_from_uri_gcp_prefixed(monkeypatch):
    uri = (
        f"google-cloud-platform:///?__extra__="
        f"{quote(json.dumps({'extra__google_cloud_platform__keyfile_dict': SAMPLE_KEY}))}"
    )
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", uri)

    assert bigquery_ci_common.keyfile() == SAMPLE_KEY


def test_keyfile_missing_env_var(monkeypatch):
    monkeypatch.delenv("AIRFLOW_CONN_GCP_GS_CONN", raising=False)

    with pytest.raises(KeyError, match="AIRFLOW_CONN_GCP_GS_CONN"):
        bigquery_ci_common.keyfile()


def test_keyfile_missing_keyfile_dict(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"key_path": "/path/to/key.json"}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    with pytest.raises(SystemExit, match="AIRFLOW_CONN_GCP_GS_CONN has no keyfile_dict"):
        bigquery_ci_common.keyfile()


def test_keyfile_empty_keyfile_dict(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": {}}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    with pytest.raises(SystemExit, match="AIRFLOW_CONN_GCP_GS_CONN has no keyfile_dict"):
        bigquery_ci_common.keyfile()


def test_keyfile_custom_env_var(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": SAMPLE_KEY}})
    monkeypatch.setenv("CUSTOM_GCP_CONN", conn_json)

    assert bigquery_ci_common.keyfile("CUSTOM_GCP_CONN") == SAMPLE_KEY


def test_keyfile_custom_env_var_missing_key(monkeypatch):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {}})
    monkeypatch.setenv("CUSTOM_GCP_CONN", conn_json)

    with pytest.raises(SystemExit, match="CUSTOM_GCP_CONN has no keyfile_dict"):
        bigquery_ci_common.keyfile("CUSTOM_GCP_CONN")


def test_get_keyfile_alias():
    assert bigquery_ci_common.get_keyfile is bigquery_ci_common.keyfile


@pytest.mark.parametrize(
    "script_name,args",
    [
        ("bigquery_ci_dataset.py", ["--help"]),
        ("bigquery_ci_keyfile.py", []),
        ("bigquery_ci_profiles.py", []),
    ],
)
def test_scripts_run_with_python_isolated_mode(script_name, args):
    """Verify that python -I can import bigquery_ci_common without ModuleNotFoundError."""
    script_path = SCRIPTS_DIR / script_name
    result = subprocess.run(
        [sys.executable, "-I", str(script_path), *args],
        capture_output=True,
        text=True,
    )
    assert "ModuleNotFoundError" not in result.stderr


def test_bigquery_ci_keyfile_main(monkeypatch, tmp_path, capsys):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": SAMPLE_KEY}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    keyfile_path = tmp_path / "sa_key.json"
    exit_code = bigquery_ci_keyfile.main([str(keyfile_path)])

    assert exit_code == 0
    assert json.loads(keyfile_path.read_text()) == SAMPLE_KEY

    stdout = capsys.readouterr().out
    assert "::add-mask::key-id-12345" in stdout
    assert "wrote the service account key" in stdout


def test_bigquery_ci_profiles_rewrite(monkeypatch, tmp_path):
    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": SAMPLE_KEY}})
    monkeypatch.setenv("AIRFLOW_CONN_GCP_GS_CONN", conn_json)

    profiles_content = {
        "my_profile": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": "localhost",
                    "threads": 4,
                },
                "other": {
                    "type": "snowflake",
                },
            },
        }
    }
    profiles_path = tmp_path / "profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles_content))

    exit_code = bigquery_ci_profiles.main([str(profiles_path)])
    assert exit_code == 0

    rewritten = yaml.safe_load(profiles_path.read_text())
    dev_output = rewritten["my_profile"]["outputs"]["dev"]
    assert dev_output["type"] == "bigquery"
    assert dev_output["method"] == "service-account-json"
    assert dev_output["keyfile_json"] == SAMPLE_KEY
    assert dev_output["threads"] == 4
    # Snowflake output should be unchanged
    assert rewritten["my_profile"]["outputs"]["other"]["type"] == "snowflake"


def test_scripts_run_with_python_isolated_mode_end_to_end(tmp_path):
    """Verify that helper scripts execute successfully end-to-end under python -I with valid args."""
    import os

    conn_json = json.dumps({"conn_type": "google_cloud_platform", "extra": {"keyfile_dict": SAMPLE_KEY}})
    env = {**os.environ, "AIRFLOW_CONN_GCP_GS_CONN": conn_json}

    # Test bigquery_ci_keyfile.py
    keyfile_path = tmp_path / "sa_key_isolated.json"
    res = subprocess.run(
        [sys.executable, "-I", str(SCRIPTS_DIR / "bigquery_ci_keyfile.py"), str(keyfile_path)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res.returncode == 0
    assert json.loads(keyfile_path.read_text()) == SAMPLE_KEY

    # Test bigquery_ci_profiles.py
    profiles_path = tmp_path / "profiles_isolated.yml"
    profiles_content = {
        "my_profile": {
            "target": "dev",
            "outputs": {
                "dev": {"type": "postgres", "host": "localhost", "threads": 4},
            },
        }
    }
    profiles_path.write_text(yaml.safe_dump(profiles_content))
    res = subprocess.run(
        [sys.executable, "-I", str(SCRIPTS_DIR / "bigquery_ci_profiles.py"), str(profiles_path)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res.returncode == 0
    rewritten = yaml.safe_load(profiles_path.read_text())
    assert rewritten["my_profile"]["outputs"]["dev"]["type"] == "bigquery"
