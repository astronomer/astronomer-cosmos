import pytest


def pytest_addoption(parser):
    parser.addoption("--package", action="store")


@pytest.fixture(scope="session")
def package(request):
    package_value = request.config.option.package
    if package_value is None:
        pytest.skip()
    return package_value
