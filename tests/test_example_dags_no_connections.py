from __future__ import annotations

from functools import cache
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])

IGNORED_DAG_FILES = [
    "performance_dag.py",
    "jaffle_shop_kubernetes.py",
    "jaffle_shop_watcher_kubernetes.py",
    "cosmos_manifest_selectors_example.py",
    "cross_project_dbt_ls_dag.py",
]


@cache
def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

        # Ignore Async DAG for dbt <=1.5
        if DBT_VERSION <= Version("1.5.0"):
            file.writelines(["simple_dag_async.py\n"])

        if DBT_VERSION < Version("1.5.0"):
            file.writelines(["example_source_rendering.py\n"])

        if DBT_VERSION < Version("1.6.0"):
            file.writelines(["example_model_version.py\n"])
            file.writelines(["example_operators.py\n"])
        # cosmos_profile_mapping uses the automatic profile rendering from an Airflow connection.
        # so we can't parse that without live connections
        for file_name in ["cosmos_profile_mapping.py"]:
            print(f"Adding {file_name} to .airflowignore")
            file.write(f"{file_name}\n")

        if AIRFLOW_VERSION == Version("2.9.0"):
            file.writelines("example_cosmos_python_models\n")

        if AIRFLOW_VERSION >= Version("3.0.0"):
            file.writelines("example_cosmos_cleanup_dag.py\n")

    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
    return db


def get_dag_ids() -> list[str]:
    dag_bag = get_dag_bag()
    return dag_bag.dag_ids


@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_parse_dags_without_connections(dag_id: str):
    dag_bag = get_dag_bag()

    assert dag_bag.dags
    assert not dag_bag.import_errors

    dag = dag_bag.get_dag(dag_id)
    assert dag is not None
