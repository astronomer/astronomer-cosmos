from __future__ import annotations

import os
from functools import cache
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from dbt.version import get_installed_version as get_dbt_version
from packaging.version import Version

from cosmos.constants import AIRFLOW_VERSION

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
DBT_VERSION = Version(get_dbt_version().to_version_string()[1:])
KUBERNETES_DAGS = ["jaffle_shop_kubernetes", "jaffle_shop_watcher_kubernetes"]

IGNORED_DAG_FILES = ["performance_dag.py", "jaffle_shop_kubernetes.py", "jaffle_shop_watcher_kubernetes.py"]


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@cache
def get_dag_bag() -> DagBag:  # noqa: C901
    """Create a DagBag by adding the files that are not supported to .airflowignore"""

    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

        if DBT_VERSION < Version("1.6.0"):
            file.writelines(["example_model_version.py\n"])
            file.writelines(["example_operators.py\n"])

        if DBT_VERSION < Version("1.5.0"):
            file.writelines(["example_source_rendering.py\n"])

        # Disabling these DAGs temporarily due to an Airflow 3 bug on processing DatasetAlias that contain non-ASCII characters:
        # https://github.com/apache/airflow/issues/51566
        # https://github.com/astronomer/astronomer-cosmos/issues/1802
        if AIRFLOW_VERSION >= Version("3.0.0"):
            file.writelines("example_source_rendering.py\n")
            file.writelines("basic_cosmos_task_group_different_owners.py\n")

    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
    return db


def get_dag_bag_single_dag(single_dag: str) -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    # add everything to airflow ignore that isn't performance_dag.py
    with open(AIRFLOW_IGNORE_FILE, "w+") as f:
        for file in EXAMPLE_DAGS_DIR.iterdir():
            if file.is_file() and file.suffix == ".py":
                if file.name != single_dag:
                    print(f"Adding {file.name} to .airflowignore")
                    f.write(f"{file.name}\n")
    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
    return db


def get_dagbag_depending_on_single_dag() -> DagBag:
    """Return DagBag for a single DAG or for all DAGs, depending on environment variable TEST_SINGLE_DAG"""
    single_dag = os.getenv("TEST_SINGLE_DAG")
    if single_dag:
        dag_bag = get_dag_bag_single_dag(single_dag)
    else:
        dag_bag = get_dag_bag()
    return dag_bag


def get_dag_ids() -> list[str]:
    dag_bag = get_dagbag_depending_on_single_dag()
    return dag_bag.dag_ids


def run_dag(dag_id: str):
    dag_bag = get_dagbag_depending_on_single_dag()
    dag = dag_bag.get_dag(dag_id)
    assert dag
    test_utils.run_dag(dag)


@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_example_dag(session, dag_id: str):
    if dag_id in KUBERNETES_DAGS:
        return
    run_dag(dag_id)


@pytest.mark.skipif(
    AIRFLOW_VERSION >= Version("3.1.0"),  # TODO: Fix https://github.com/astronomer/astronomer-cosmos/issues/2045
    reason="TODO: Fix https://github.com/astronomer/astronomer-cosmos/issues/2045",
)
@patch.dict(
    os.environ,
    {"AIRFLOW__COSMOS__ENABLE_SETUP_ASYNC_TASK": "false", "AIRFLOW__COSMOS__ENABLE_TEARDOWN_ASYNC_TASK": "false"},
)
@pytest.mark.integration
def test_async_example_dag_without_setup_task(session, monkeypatch):
    async_dag_id = "simple_dag_async"
    run_dag(async_dag_id)
