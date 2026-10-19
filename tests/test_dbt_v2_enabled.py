from pathlib import Path

import pytest

from tests import dbt_v2_enabled

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"


def test_enabled_dag_files_exist():
    missing = sorted(name for name in dbt_v2_enabled.ENABLED_DAG_FILES if not (EXAMPLE_DAGS_DIR / name).is_file())
    assert not missing, f"Enabled for dbt v2 but not in dev/dags: {missing}"


def test_enabled_test_files_exist():
    enabled = dbt_v2_enabled.ENABLED_TEST_FILES or frozenset()
    missing = sorted(path for path in enabled if not (Path(__file__).parent.parent / path).is_file())
    assert not missing, f"Enabled for dbt v2 but not found: {missing}"


def test_no_dag_files_are_ignored_outside_the_dbt_v2_job(monkeypatch):
    monkeypatch.delenv("COSMOS_TEST_ADAPTER", raising=False)
    assert dbt_v2_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR) == []


@pytest.mark.parametrize("adapter", ["postgres", ""])
def test_no_dag_files_are_ignored_for_other_adapters(monkeypatch, adapter):
    monkeypatch.setenv("COSMOS_TEST_ADAPTER", adapter)
    assert dbt_v2_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR) == []


def test_only_the_enabled_dag_files_are_kept_in_the_dbt_v2_job(monkeypatch):
    monkeypatch.setenv("COSMOS_TEST_ADAPTER", "bigquery")
    ignored = set(dbt_v2_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR))
    all_dag_files = {path.name for path in EXAMPLE_DAGS_DIR.glob("*.py")}
    assert ignored == all_dag_files - dbt_v2_enabled.ENABLED_DAG_FILES
    assert not ignored & dbt_v2_enabled.ENABLED_DAG_FILES
