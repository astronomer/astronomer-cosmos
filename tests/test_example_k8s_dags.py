import os
from pathlib import Path

import pytest
from airflow import __version__ as airflow_version
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from packaging import version

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"

KUBERNETES_DAG_FILES = ["jaffle_shop_kubernetes.py"]


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


def get_all_dag_files():
    python_files = []
    for file in os.listdir(EXAMPLE_DAGS_DIR):
        if file.endswith(".py") and file not in KUBERNETES_DAG_FILES:
            python_files.append(file)

    with open(AIRFLOW_IGNORE_FILE, "w+") as dag_ignorefile:
        dag_ignorefile.writelines([f"{file}\n" for file in python_files])


# TODO: Add compatibility for Airflow 3.0. Issue: #1705.
#  Comment: https://github.com/astronomer/astronomer-cosmos/issues/1705#issuecomment-2834926490
#  Hint: Check if we can dag.test instead for Airflow 3.0 here.
@pytest.mark.skipif(version.parse(airflow_version).major == 3, reason="Add compatibility for 3.0")
@pytest.mark.integration
def test_example_dag_kubernetes(session):
    get_all_dag_files()
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    # for dag_id in KUBERNETES_DAG_FILES:
    dag = db.get_dag("jaffle_shop_kubernetes")
    test_utils.run_dag(dag)
