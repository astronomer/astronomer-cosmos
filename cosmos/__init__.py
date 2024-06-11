# type: ignore # ignores "Cannot assign to a type" MyPy error

"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""
__version__ = "1.5.0a6"


from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import (
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.log import get_logger
from cosmos.operators.lazy_load import MissingPackage
from cosmos.operators.local import (
    DbtBuildLocalOperator,
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

try:
    from cosmos.operators.azure_container_instance import (
        DbtLSAzureContainerInstanceOperator,
        DbtRunAzureContainerInstanceOperator,
        DbtRunOperationAzureContainerInstanceOperator,
        DbtSeedAzureContainerInstanceOperator,
        DbtSnapshotAzureContainerInstanceOperator,
        DbtTestAzureContainerInstanceOperator,
    )
except ImportError:
    DbtLSAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtLSAzureContainerInstanceOperator", "azure-container-instance"
    )
    DbtRunAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtRunAzureContainerInstanceOperator", "azure-container-instance"
    )
    DbtRunOperationAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtRunOperationAzureContainerInstanceOperator",
        "azure-container-instance",
    )
    DbtSeedAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtSeedAzureContainerInstanceOperator", "azure-container-instance"
    )
    DbtSnapshotAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtSnapshotAzureContainerInstanceOperator",
        "azure-container-instance",
    )
    DbtTestAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtTestAzureContainerInstanceOperator", "azure-container-instance"
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
    "DbtBuildLocalOperator",
    "DbtDepsLocalOperator",
    "DbtSnapshotLocalOperator",
    "DbtDag",
    "DbtTaskGroup",
    "DbtLSDockerOperator",
    "DbtRunOperationDockerOperator",
    "DbtRunDockerOperator",
    "DbtSeedDockerOperator",
    "DbtTestDockerOperator",
    "DbtBuildDockerOperator",
    "DbtSnapshotDockerOperator",
    "DbtLSKubernetesOperator",
    "DbtRunOperationKubernetesOperator",
    "DbtRunKubernetesOperator",
    "DbtSeedKubernetesOperator",
    "DbtTestKubernetesOperator",
    "DbtBuildKubernetesOperator",
    "DbtSnapshotKubernetesOperator",
    "DbtLSAzureContainerInstanceOperator",
    "DbtRunOperationAzureContainerInstanceOperator",
    "DbtRunAzureContainerInstanceOperator",
    "DbtSeedAzureContainerInstanceOperator",
    "DbtTestAzureContainerInstanceOperator",
    "DbtSnapshotAzureContainerInstanceOperator",
    "ExecutionMode",
    "LoadMode",
    "TestBehavior",
]

"""
Required provider info for using Airflow config for configuration
"""


def get_provider_info():
    return {
        "package-name": "astronomer-cosmos",  # Required
        "name": "Astronomer Cosmos",  # Required
        "description": "Astronomer Cosmos is a library for rendering dbt workflows in Airflow. Contains dags, task groups, and operators.",  # Required
        "versions": [__version__],  # Required
        "config": {
            "cosmos": {
                "description": None,
                "options": {
                    "propagate_logs": {
                        "description": "Enable log propagation from Cosmos custom logger\n",
                        "version_added": "1.3.0a1",
                        "type": "boolean",
                        "example": None,
                        "default": "True",
                    },
                },
            },
        },
    }
