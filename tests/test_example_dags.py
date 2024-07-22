from __future__ import annotations

from pathlib import Path

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

from cosmos.constants import PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])
AIRFLOW_VERSION = Version(airflow.__version__)

MIN_VER_DAG_FILE: dict[str, list[str]] = {
    "2.4": ["cosmos_seed_dag.py"],
    "2.8": ["cosmos_manifest_example.py"],
}

IGNORED_DAG_FILES = ["performance_dag.py"]

# Sort descending based on Versions and convert string to an actual version
MIN_VER_DAG_FILE_VER: dict[Version, list[str]] = {
    Version(version): MIN_VER_DAG_FILE[version] for version in sorted(MIN_VER_DAG_FILE, key=Version, reverse=True)
}


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@cache
def get_dag_bag(in_kube: bool = False) -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    if AIRFLOW_VERSION in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS:
        return DagBag(dag_folder=None, include_examples=False)

    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if AIRFLOW_VERSION < min_version:
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file}\n" for file in files])

        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

        # The dbt sqlite adapter is only available until dbt 1.4
        if DBT_VERSION >= Version("1.5.0"):
            file.writelines(["example_cosmos_sources.py\n"])
        if DBT_VERSION < Version("1.6.0"):
            file.writelines(["example_model_version.py\n"])

        if not in_kube:
            file.writelines(["jaffle_shop_kubernetes.py\n"])

    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
    return db


def get_dag_ids(in_kube: bool = False) -> list[str]:
    dag_bag = get_dag_bag(in_kube)
    return dag_bag.dag_ids


@pytest.mark.skipif(
    AIRFLOW_VERSION in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow 2.9.0 and 2.9.1 have a breaking change in Dataset URIs, and Cosmos errors if `emit_datasets` is not False",
)
@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_example_dag(session, dag_id: str):
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag(dag_id)
    test_utils.run_dag(dag)
