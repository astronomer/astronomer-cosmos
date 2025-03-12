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
from packaging.version import Version

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
ALL_FILES_TO_IGNORE = [
    f.name for f in EXAMPLE_DAGS_DIR.iterdir() if f.is_file() and f.suffix == ".py" and f.name != "simple_dag_async.py"
]

AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
AIRFLOW_VERSION = Version(airflow.__version__)


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
