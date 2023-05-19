from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent.parent / "dev/dags"
EXAMPLE_CONN_FILE = Path(__file__).parent / "test-connections.yaml"


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    dag_bag = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert dag_bag.dags
    import pdb

    pdb.set_trace()
    assert not dag_bag.import_errors
    return dag_bag


dag_bag = get_dag_bag()


@pytest.mark.integration
@pytest.mark.parametrize("dag_id", dag_bag.dag_ids)
def test_example_dag(session, dag_id: str):
    dag = dag_bag.get_dag(dag_id)
    test_utils.run_dag(dag, conn_file_path=EXAMPLE_CONN_FILE)
