from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import patch

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache


import airflow
import pytest
import sqlalchemy
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
KUBERNETES_DAGS = ["jaffle_shop_kubernetes"]

MIN_VER_DAG_FILE: dict[str, list[str]] = {
    "2.4": ["cosmos_seed_dag.py"],
    "2.8": ["cosmos_manifest_example.py", "simple_dag_async.py", "cosmos_callback_dag.py"],
}

IGNORED_DAG_FILES = ["performance_dag.py", "jaffle_shop_kubernetes.py"]


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
def get_dag_bag() -> DagBag:
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

        # Ignore Async DAG for dbt <=1.5
        if DBT_VERSION <= Version("1.5.0"):
            file.writelines(["simple_dag_async.py\n"])

        if DBT_VERSION < Version("1.6.0"):
            file.writelines(["example_model_version.py\n"])
            file.writelines(["example_operators.py\n"])

        if DBT_VERSION < Version("1.5.0"):
            file.writelines(["example_source_rendering.py\n"])

        if AIRFLOW_VERSION < Version("2.8.0"):
            file.writelines("example_cosmos_dbt_build.py\n")

    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
    return db


def get_dag_ids() -> list[str]:
    dag_bag = get_dag_bag()
    return dag_bag.dag_ids


def run_dag(dag_id: str):
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag(dag_id)

    # This feature is available since Airflow 2.5 and we've backported it in Cosmos:
    # https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#airflow-2-5-0-2022-12-02
    if AIRFLOW_VERSION >= Version("2.5"):
        if AIRFLOW_VERSION not in (Version("2.10.0"), Version("2.10.1"), Version("2.10.2")):
            dag.test()
        else:
            # This is a work around until we fix the issue in Airflow:
            # https://github.com/apache/airflow/issues/42495
            """
            FAILED tests/test_example_dags.py::test_example_dag[example_model_version] - sqlalchemy.exc.PendingRollbackError:
            This Session's transaction has been rolled back due to a previous exception during flush. To begin a new transaction with this Session, first issue Session.rollback().
            Original exception was: Can't flush None value found in collection DatasetModel.aliases (Background on this error at: https://sqlalche.me/e/14/7s2a)
            FAILED tests/test_example_dags.py::test_example_dag[basic_cosmos_dag]
            FAILED tests/test_example_dags.py::test_example_dag[cosmos_profile_mapping]
            FAILED tests/test_example_dags.py::test_example_dag[user_defined_profile]
            """
            try:
                dag.test()
            except sqlalchemy.exc.PendingRollbackError:
                warnings.warn(
                    "Early versions of Airflow 2.10 have issues when running the test command with DatasetAlias / Datasets"
                )
    else:
        test_utils.run_dag(dag)


@pytest.mark.skipif(
    AIRFLOW_VERSION in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="Airflow 2.9.0 and 2.9.1 have a breaking change in Dataset URIs, and Cosmos errors if `emit_datasets` is not False",
)
@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_example_dag(session, dag_id: str):
    if dag_id in KUBERNETES_DAGS:
        return
    run_dag(dag_id)


async_dag_ids = ["simple_dag_async"]


@pytest.mark.skipif(
    AIRFLOW_VERSION < Version("2.8") or AIRFLOW_VERSION in PARTIALLY_SUPPORTED_AIRFLOW_VERSIONS,
    reason="See PR: https://github.com/apache/airflow/pull/34585 and Airflow 2.9.0 and 2.9.1 have a breaking change in Dataset URIs, and Cosmos errors if `emit_datasets` is not False",
)
@pytest.mark.integration
@patch("cosmos.operators.local.settings.enable_setup_async_task", False)
@patch("cosmos.operators.local.settings.enable_teardown_async_task", False)
@patch("cosmos.operators._asynchronous.bigquery.settings.enable_setup_async_task", False)
def test_async_example_dag_without_setup_task(session):
    for dag_id in async_dag_ids:
        run_dag(dag_id)
