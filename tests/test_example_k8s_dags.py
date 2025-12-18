import os
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from cosmos.constants import _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"

KUBERNETES_DAG_FILES = ["jaffle_shop_kubernetes.py", "jaffle_shop_watcher_kubernetes.py"]


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


@pytest.mark.integration
def test_example_dag_kubernetes(session):
    get_all_dag_files()
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert not db.import_errors
    dag = db.get_dag("jaffle_shop_kubernetes")
    test_utils.run_dag(dag)


from airflow.providers.cncf.kubernetes import __version__ as airflow_k8s_provider_version
from packaging.version import Version


@pytest.mark.skipif(
    Version(airflow_k8s_provider_version) < _K8s_WATCHER_MIN_K8S_PROVIDER_VERSION,
    reason="This feature is only available for K8s provider 10.0 and above",
)
@pytest.mark.integration
def test_example_dag_watcher_kubernetes(session):
    get_all_dag_files()
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    dag = db.get_dag("jaffle_shop_watcher_kubernetes")
    assert not db.import_errors
    assert dag is not None
    test_utils.run_dag(dag)
