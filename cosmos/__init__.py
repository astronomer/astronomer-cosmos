# type: ignore # ignores "Cannot assign to a type" MyPy error

"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""
__version__ = "1.2.0a1"

from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import (
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.log import get_logger
from cosmos.operators.lazy_load import MissingPackage
from cosmos.operators.local import (
    DbtDepsLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

logger = get_logger()

try:
    from cosmos.operators.docker import (
        DbtLSDockerOperator,
        DbtRunDockerOperator,
        DbtRunOperationDockerOperator,
        DbtSeedDockerOperator,
        DbtSnapshotDockerOperator,
        DbtTestDockerOperator,
    )
except ImportError:
    DbtLSDockerOperator = MissingPackage("cosmos.operators.docker.DbtLSDockerOperator", "docker")
    DbtRunDockerOperator = MissingPackage("cosmos.operators.docker.DbtRunDockerOperator", "docker")
    DbtRunOperationDockerOperator = MissingPackage(
        "cosmos.operators.docker.DbtRunOperationDockerOperator",
        "docker",
    )
    DbtSeedDockerOperator = MissingPackage("cosmos.operators.docker.DbtSeedDockerOperator", "docker")
    DbtSnapshotDockerOperator = MissingPackage("cosmos.operators.docker.DbtSnapshotDockerOperator", "docker")
    DbtTestDockerOperator = MissingPackage("cosmos.operators.docker.DbtTestDockerOperator", "docker")

try:
    from cosmos.operators.kubernetes import (
        DbtLSKubernetesOperator,
        DbtRunKubernetesOperator,
        DbtRunOperationKubernetesOperator,
        DbtSeedKubernetesOperator,
        DbtSnapshotKubernetesOperator,
        DbtTestKubernetesOperator,
    )
except ImportError:
    logger.debug("To import Kubernetes modules, install astronomer-cosmos[kubernetes].", stack_info=True)
    DbtLSKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtLSKubernetesOperator",
        "kubernetes",
    )
    DbtRunKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtRunKubernetesOperator",
        "kubernetes",
    )
    DbtRunOperationKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtRunOperationKubernetesOperator",
        "kubernetes",
    )
    DbtSeedKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtSeedKubernetesOperator",
        "kubernetes",
    )
    DbtSnapshotKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtSnapshotKubernetesOperator",
        "kubernetes",
    )
    DbtTestKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtTestKubernetesOperator",
        "kubernetes",
    )

__all__ = [
    "ProjectConfig",
    "ProfileConfig",
    "ExecutionConfig",
    "RenderConfig",
    "DbtLSLocalOperator",
    "DbtRunOperationLocalOperator",
    "DbtRunLocalOperator",
    "DbtSeedLocalOperator",
    "DbtTestLocalOperator",
    "DbtDepsLocalOperator",
    "DbtSnapshotLocalOperator",
    "DbtDag",
    "DbtTaskGroup",
    "DbtLSDockerOperator",
    "DbtRunOperationDockerOperator",
    "DbtRunDockerOperator",
    "DbtSeedDockerOperator",
    "DbtTestDockerOperator",
    "DbtSnapshotDockerOperator",
    "DbtLSKubernetesOperator",
    "DbtRunOperationKubernetesOperator",
    "DbtRunKubernetesOperator",
    "DbtSeedKubernetesOperator",
    "DbtTestKubernetesOperator",
    "DbtSnapshotKubernetesOperator",
    "ExecutionMode",
    "LoadMode",
    "TestBehavior",
]
