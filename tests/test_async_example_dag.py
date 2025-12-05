# We already have tests/test_example_dags.py, but it doesn’t run against multiple dbt versions in CI.
# Some dbt versions have shown parsing issues with certain example DAGs — something we may need to address over time.
# With PR #1535, the goal is to test the async example DAG across multiple dbt versions. To prevent the CI job from
# failing early due to unrelated DAG parsing errors, PR #1535 introduces this new test_async_example_dag.py file.
# This file replicates tests/test_example_dags.py but excludes all DAGs except simple_async_dag by adding them to
# .airflowignore. This ensures the CI job focuses solely on testing simple_async_dag over multiple dbt versions
# without being disrupted by other DAG parsing issues.

from __future__ import annotations

from pathlib import Path

from functools import cache


import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
ALL_FILES_TO_IGNORE = [
    f.name for f in EXAMPLE_DAGS_DIR.iterdir() if f.is_file() and f.suffix == ".py" and f.name != "simple_dag_async.py"
]

AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"


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

    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for dagfile in ALL_FILES_TO_IGNORE:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

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
def test_example_dag(session, dag_id: str):
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag(dag_id)
    dag.test()
