import ast
from pathlib import Path

import pytest

from tests import dbt_oss_enabled

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"


def test_enabled_dag_files_exist():
    missing = sorted(name for name in dbt_oss_enabled.ENABLED_DAG_FILES if not (EXAMPLE_DAGS_DIR / name).is_file())
    assert not missing, f"Enabled for dbt-oss but not in dev/dags: {missing}"


def test_no_dag_files_are_ignored_outside_the_dbt_oss_job(monkeypatch):
    monkeypatch.delenv("COSMOS_TEST_ADAPTER", raising=False)
    assert dbt_oss_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR) == []


@pytest.mark.parametrize("adapter", ["postgres", ""])
def test_no_dag_files_are_ignored_for_other_adapters(monkeypatch, adapter):
    monkeypatch.setenv("COSMOS_TEST_ADAPTER", adapter)
    assert dbt_oss_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR) == []


def test_only_the_enabled_dag_files_are_kept_in_the_dbt_oss_job(monkeypatch):
    monkeypatch.setenv("COSMOS_TEST_ADAPTER", "bigquery")
    ignored = set(dbt_oss_enabled.dag_files_to_ignore(EXAMPLE_DAGS_DIR))
    all_dag_files = {path.name for path in EXAMPLE_DAGS_DIR.glob("*.py")}
    assert ignored == all_dag_files - dbt_oss_enabled.ENABLED_DAG_FILES
    assert not ignored & dbt_oss_enabled.ENABLED_DAG_FILES


def test_skipped_tests_point_at_existing_test_files():
    repo_root = Path(__file__).parent.parent
    missing = sorted(
        node_id for node_id in dbt_oss_enabled.SKIPPED_TESTS if not (repo_root / node_id.split("::")[0]).is_file()
    )
    assert not missing, f"Skipped for dbt-oss but the test file is not found: {missing}"


def _test_function_names(test_file: Path) -> set[str]:
    return {
        node.name
        for node in ast.walk(ast.parse(test_file.read_text()))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_skipped_tests_point_at_existing_test_functions():
    # A skipped test that was renamed or deleted is otherwise ignored without any signal. The name is checked, not the
    # parametrize id in brackets: the DAG ids of test_example_dag[...] depend on the Airflow version and on which DAG
    # files are enabled, so those cannot be checked without collecting.
    repo_root = Path(__file__).parent.parent
    missing = sorted(
        node_id
        for node_id in dbt_oss_enabled.SKIPPED_TESTS
        if node_id.split("::")[-1].split("[")[0] not in _test_function_names(repo_root / node_id.split("::")[0])
    )
    assert not missing, f"Skipped for dbt-oss but no such test function exists: {missing}"


def test_test_function_names_finds_module_level_and_nested_functions(tmp_path):
    test_file = tmp_path / "test_sample.py"
    test_file.write_text("def test_a():\n    pass\n\n\nclass TestB:\n    def test_c(self):\n        pass\n")

    assert _test_function_names(test_file) == {"test_a", "test_c"}


def test_skipped_tests_have_a_reason():
    assert all(reason.strip() for reason in dbt_oss_enabled.SKIPPED_TESTS.values())


def test_skipped_example_dag_tests_belong_to_enabled_dag_files():
    # A skipped test_example_dag[...] entry for a DAG whose file is not enabled would never be collected.
    skipped_dag_ids = {
        node_id.split("test_example_dag[")[1].rstrip("]")
        for node_id in dbt_oss_enabled.SKIPPED_TESTS
        if "::test_example_dag[" in node_id
    }
    enabled_text = "\n".join((EXAMPLE_DAGS_DIR / name).read_text() for name in dbt_oss_enabled.ENABLED_DAG_FILES)
    assert all(f'dag_id="{dag_id}"' in enabled_text for dag_id in skipped_dag_ids)
