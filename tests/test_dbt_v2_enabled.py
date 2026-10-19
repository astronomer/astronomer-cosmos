from pathlib import Path

import pytest

from tests import dbt_v2_enabled

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"


def test_enabled_dag_files_exist():
    missing = sorted(name for name in dbt_v2_enabled.ENABLED_DAG_FILES if not (EXAMPLE_DAGS_DIR / name).is_file())
    assert not missing, f"Enabled for dbt v2 but not in dev/dags: {missing}"


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


def test_skipped_tests_point_at_existing_test_files():
    repo_root = Path(__file__).parent.parent
    missing = sorted(
        node_id for node_id in dbt_v2_enabled.SKIPPED_TESTS if not (repo_root / node_id.split("::")[0]).is_file()
    )
    assert not missing, f"Skipped for dbt v2 but the test file is not found: {missing}"


def test_skipped_tests_have_a_reason():
    assert all(reason.strip() for reason in dbt_v2_enabled.SKIPPED_TESTS.values())


def test_skipped_example_dag_tests_belong_to_enabled_dag_files():
    # A skipped test_example_dag[...] entry for a DAG whose file is not enabled would never be collected.
    skipped_dag_ids = {
        node_id.split("test_example_dag[")[1].rstrip("]")
        for node_id in dbt_v2_enabled.SKIPPED_TESTS
        if "::test_example_dag[" in node_id
    }
    enabled_text = "\n".join((EXAMPLE_DAGS_DIR / name).read_text() for name in dbt_v2_enabled.ENABLED_DAG_FILES)
    assert all(f'dag_id="{dag_id}"' in enabled_text for dag_id in skipped_dag_ids)
