"""
Tests exports from the dbt provider.
"""
import importlib
import sys
from unittest import mock

import pytest

docker_operators = [
    "DbtLSDockerOperator",
    "DbtRunDockerOperator",
    "DbtRunOperationDockerOperator",
    "DbtSeedDockerOperator",
    "DbtSnapshotDockerOperator",
    "DbtTestDockerOperator",
]


@pytest.fixture(autouse=False)
def reset_docker_operators() -> None:
    """
    Remove all docker operators from sys.modules.
    """
    for key in list(sys.modules.keys()):
        if "docker" in key.lower():
            del sys.modules[key]


@pytest.mark.parametrize(
    "operator",
    docker_operators,
)
def test_failed_docker_import(operator: str, reset_docker_operators: None) -> None:
    """
    Test that the docker operators are not imported when docker is not installed.
    Should raise a RuntimeError.
    """
    with mock.patch.dict(sys.modules, {"airflow.providers.docker.operators.docker": None}):
        with pytest.raises(RuntimeError):
            from cosmos.providers import dbt

            importlib.reload(dbt)
            getattr(dbt, operator)()


kubernetes_operators = [
    "DbtLSKubernetesOperator",
    "DbtRunKubernetesOperator",
    "DbtRunOperationKubernetesOperator",
    "DbtSeedKubernetesOperator",
    "DbtSnapshotKubernetesOperator",
    "DbtTestKubernetesOperator",
]


@pytest.fixture(autouse=False)
def reset_kubernetes_operators() -> None:
    """
    Remove all kubernetes operators from sys.modules.
    """
    for key in list(sys.modules.keys()):
        if "kubernetes" in key.lower():
            del sys.modules[key]


@pytest.mark.parametrize(
    "operator",
    kubernetes_operators,
)
def test_failed_kubernetes_import(operator: str, reset_kubernetes_operators: None) -> None:
    """
    Test that the kubernetes operators are not imported when kubernetes is not installed.
    Should raise a RuntimeError.
    """
    with mock.patch.dict(
        sys.modules,
        {
            "airflow.providers.cncf.kubernetes.operators.kubernetes_pod": None,
            "airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters": None,
            "kubernetes.client.models": None,
        },
    ):
        with pytest.raises(RuntimeError):
            from cosmos.providers import dbt

            importlib.reload(dbt)
            getattr(dbt, operator)()
