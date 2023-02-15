import pytest
from airflow.models import DAG
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
    yield dag
