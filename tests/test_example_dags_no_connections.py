from __future__ import annotations

import sys
from pathlib import Path

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import airflow
import pytest
from airflow.models.dagbag import DagBag
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])
_PYTHON_VERSION = sys.version_info[:2]

MIN_VER_DAG_FILE: dict[str, list[str]] = {
    "2.4": ["cosmos_seed_dag.py"],
    "2.8": ["cosmos_manifest_example.py"],
}

IGNORED_DAG_FILES = ["performance_dag.py", "jaffle_shop_kubernetes.py"]

# Sort descending based on Versions and convert string to an actual version
MIN_VER_DAG_FILE_VER: dict[Version, list[str]] = {
    Version(version): MIN_VER_DAG_FILE[version] for version in sorted(MIN_VER_DAG_FILE, key=Version, reverse=True)
}


@cache
def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if Version(airflow.__version__) < min_version:
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file_name}\n" for file_name in files])

        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

        # Python 3.8 has reached its end of life (EOL), and dbt no longer supports this version.
        # This results in an error, as outlined in https://github.com/duckdb/dbt-duckdb/issues/488
        if _PYTHON_VERSION < (3, 9):
            file.writelines(["example_duckdb_dag.py\n"])

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
