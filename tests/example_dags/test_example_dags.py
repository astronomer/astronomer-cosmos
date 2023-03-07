from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from . import utils as test_utils


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    example_dags_dir = Path(__file__).parent.parent / "example_dags"
    airflow_ignore_file = example_dags_dir / ".airflowignore"

    print(".airflowignore contents: ")
    print(airflow_ignore_file.read_text())
    dag_bag = DagBag(example_dags_dir, include_examples=False)
    assert dag_bag.dags
    assert not dag_bag.import_errors
    return dag_bag


dag_bag = get_dag_bag()


@pytest.mark.parametrize("dag_id", dag_bag.dag_ids)
def test_example_dag(session, dag_id: str):
    dag = dag_bag.get_dag(dag_id)
    test_utils.run_dag(dag, conn_file_path="test-connections.yaml")
