# type: ignore # ignores "Cannot assign to a type" MyPy error

"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""
__version__ = "1.0.2"

from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.dataset import get_dbt_dataset
from cosmos.operators.lazy_load import MissingPackage
from cosmos.config import (
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)

from cosmos.operators.local import (
    DbtDepsLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

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
    "get_dbt_dataset",
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
