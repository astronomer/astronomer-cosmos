# type: ignore
"""
dbt support for Airflow. Contains dags, task groups, and operators.
"""

# re-export user facing utilities
from cosmos.providers.dbt.core.utils.data_aware_scheduling import get_dbt_dataset

# re-export the dag and task group
from cosmos.providers.dbt.dag import DbtDag
from cosmos.providers.dbt.task_group import DbtTaskGroup

# re-export the operators
from .core.operators.local import (
    DbtDepsLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

try:
    from .core.operators.docker import (
        DbtLSDockerOperator,
        DbtRunDockerOperator,
        DbtRunOperationDockerOperator,
        DbtSeedDockerOperator,
        DbtSnapshotDockerOperator,
        DbtTestDockerOperator,
    )
except ImportError:
    from .core.operators.lazy_load import MissingPackage

    DbtLSDockerOperator = MissingPackage("cosmos.providers.dbt.core.operators.docker.DbtLSDockerOperator", "docker")
    DbtRunDockerOperator = MissingPackage("cosmos.providers.dbt.core.operators.docker.DbtRunDockerOperator", "docker")
    DbtRunOperationDockerOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.docker.DbtRunOperationDockerOperator",
        "docker",
    )
    DbtSeedDockerOperator = MissingPackage("cosmos.providers.dbt.core.operators.docker.DbtSeedDockerOperator", "docker")
    DbtSnapshotDockerOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.docker.DbtSnapshotDockerOperator", "docker"
    )
    DbtTestDockerOperator = MissingPackage("cosmos.providers.dbt.core.operators.docker.DbtTestDockerOperator", "docker")

try:
    from .core.operators.kubernetes import (
        DbtLSKubernetesOperator,
        DbtRunKubernetesOperator,
        DbtRunOperationKubernetesOperator,
        DbtSeedKubernetesOperator,
        DbtSnapshotKubernetesOperator,
        DbtTestKubernetesOperator,
    )
except ImportError:
    from .core.operators.lazy_load import MissingPackage

    DbtLSKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtLSKubernetesOperator",
        "kubernetes",
    )
    DbtRunKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtRunKubernetesOperator",
        "kubernetes",
    )
    DbtRunOperationKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtRunOperationKubernetesOperator",
        "kubernetes",
    )
    DbtSeedKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtSeedKubernetesOperator",
        "kubernetes",
    )
    DbtSnapshotKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtSnapshotKubernetesOperator",
        "kubernetes",
    )
    DbtTestKubernetesOperator = MissingPackage(
        "cosmos.providers.dbt.core.operators.kubernetes.DbtTestKubernetesOperator",
        "kubernetes",
    )

__all__ = [
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
]
