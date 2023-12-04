from __future__ import annotations

from pathlib import Path
from functools import cache

import airflow
import pytest
from airflow.models.dagbag import DagBag
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version


EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])

MIN_VER_DAG_FILE: dict[str, list[str]] = {
    "2.4": ["cosmos_seed_dag.py"],
}

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
        a = 1 + 2
        b = 3 + 4
        a + b
        if DBT_VERSION >= Version("1.5.0"):
            file.writelines(["example_cosmos_sources.py\n"])
        if DBT_VERSION < Version("1.6.0"):
            file.writelines(["example_model_version.py\n"])
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
