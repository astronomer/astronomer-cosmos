import pytest
from airflow.models import DAG

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    # For older versions of Airflow which do not contain the EmptyOperator.
    from airflow.operators.dummy import DummyOperator as EmptyOperator

from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_test_dag"


def pytest_addoption(parser):
    parser.addoption("--package", action="store")


@pytest.fixture(scope="session")
def package(request):
    package_value = request.config.option.package
    if package_value is None:
        pytest.skip()
    return package_value


@pytest.fixture
def dag():
    """
    Creates a test DAG with default arguments.
    """
    dag = DAG(TEST_DAG_ID, start_date=datetime(2022, 1, 1))
    with dag:
        EmptyOperator(task_id="foo")
    yield dag
