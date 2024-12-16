# type: ignore # ignores "Cannot assign to a type" MyPy error

"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""

__version__ = "1.8.0a1"


from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import (
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import (
    DbtResourceType,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)
from cosmos.log import get_logger
from cosmos.operators.lazy_load import MissingPackage
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtDepsLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtTestLocalOperator,
)

logger = get_logger(__name__)

try:
    from cosmos.operators.docker import (
        DbtBuildDockerOperator,
        DbtCloneDockerOperator,
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
        DbtBuildKubernetesOperator,
        DbtCloneKubernetesOperator,
        DbtLSKubernetesOperator,
        DbtRunKubernetesOperator,
        DbtRunOperationKubernetesOperator,
        DbtSeedKubernetesOperator,
        DbtSnapshotKubernetesOperator,
        DbtTestKubernetesOperator,
    )
except ImportError:
    logger.debug("To import Kubernetes modules, install astronomer-cosmos[kubernetes].", stack_info=True)
    DbtBuildKubernetesOperator = MissingPackage(
        "cosmos.operators.kubernetes.DbtBuildKubernetesOperator",
        "kubernetes",
    )
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
        DbtBuildAzureContainerInstanceOperator,
        DbtCloneAzureContainerInstanceOperator,
        DbtLSAzureContainerInstanceOperator,
        DbtRunAzureContainerInstanceOperator,
        DbtRunOperationAzureContainerInstanceOperator,
        DbtSeedAzureContainerInstanceOperator,
        DbtSnapshotAzureContainerInstanceOperator,
        DbtTestAzureContainerInstanceOperator,
    )
except ImportError:
    DbtBuildAzureContainerInstanceOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtBuildAzureContainerInstanceOperator", "azure-container-instance"
    )
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


try:
    from cosmos.operators.aws_eks import (
        DbtBuildAwsEksOperator,
        DbtCloneAwsEksOperator,
        DbtLSAwsEksOperator,
        DbtRunAwsEksOperator,
        DbtRunOperationAwsEksOperator,
        DbtSeedAwsEksOperator,
        DbtSnapshotAwsEksOperator,
        DbtTestAwsEksOperator,
    )
except ImportError:
    DbtBuildAwsEksOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtBuildAwsEksOperator", "aws_eks"
    )
    DbtLSAwsEksOperator = MissingPackage("cosmos.operators.azure_container_instance.DbtLSAwsEksOperator", "aws_eks")
    DbtRunAwsEksOperator = MissingPackage("cosmos.operators.azure_container_instance.DbtRunAwsEksOperator", "aws_eks")
    DbtRunOperationAwsEksOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtRunOperationAwsEksOperator",
        "aws_eks",
    )
    DbtSeedAwsEksOperator = MissingPackage("cosmos.operators.azure_container_instance.DbtSeedAwsEksOperator", "aws_eks")
    DbtSnapshotAwsEksOperator = MissingPackage(
        "cosmos.operators.azure_container_instance.DbtSnapshotAwsEksOperator",
        "aws_eks",
    )
    DbtTestAwsEksOperator = MissingPackage("cosmos.operators.azure_container_instance.DbtTestAwsEksOperator", "aws_eks")


try:
    from cosmos.operators.gcp_cloud_run_job import (
        DbtBuildGcpCloudRunJobOperator,
        DbtCloneGcpCloudRunJobOperator,
        DbtLSGcpCloudRunJobOperator,
        DbtRunGcpCloudRunJobOperator,
        DbtRunOperationGcpCloudRunJobOperator,
        DbtSeedGcpCloudRunJobOperator,
        DbtSnapshotGcpCloudRunJobOperator,
        DbtTestGcpCloudRunJobOperator,
    )
except (ImportError, AttributeError):
    DbtBuildGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtBuildGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtLSGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtLSGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtRunGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtRunGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtRunOperationGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtRunOperationGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtSeedGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtSeedGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtSnapshotGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtSnapshotGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )
    DbtTestGcpCloudRunJobOperator = MissingPackage(
        "cosmos.operators.gcp_cloud_run_job.DbtTestGcpCloudRunJobOperator", "gcp-cloud-run-job"
    )


__all__ = [
    "ProjectConfig",
    "ProfileConfig",
    "ExecutionConfig",
    "RenderConfig",
    "DbtDag",
    "DbtTaskGroup",
    "ExecutionMode",
    "LoadMode",
    "TestBehavior",
    "InvocationMode",
    "TestIndirectSelection",
    "SourceRenderingBehavior",
    "DbtResourceType",
    # Local Execution Mode
    "DbtBuildLocalOperator",
    "DbtCloneLocalOperator",
    "DbtDepsLocalOperator",  # deprecated, to be delete in Cosmos 2.x
    "DbtLSLocalOperator",
    "DbtRunLocalOperator",
    "DbtRunOperationLocalOperator",
    "DbtSeedLocalOperator",
    "DbtSnapshotLocalOperator",
    "DbtTestLocalOperator",
    # Docker Execution Mode
    "DbtBuildDockerOperator",
    "DbtCloneDockerOperator",
    "DbtLSDockerOperator",
    "DbtRunDockerOperator",
    "DbtRunOperationDockerOperator",
    "DbtSeedDockerOperator",
    "DbtSnapshotDockerOperator",
    "DbtTestDockerOperator",
    # Kubernetes Execution Mode
    "DbtBuildKubernetesOperator",
    "DbtCloneKubernetesOperator",
    "DbtLSKubernetesOperator",
    "DbtRunKubernetesOperator",
    "DbtRunOperationKubernetesOperator",
    "DbtSeedKubernetesOperator",
    "DbtSnapshotKubernetesOperator",
    "DbtTestKubernetesOperator",
    # Azure Container Instance Execution Mode
    "DbtBuildAzureContainerInstanceOperator",
    "DbtCloneAzureContainerInstanceOperator",
    "DbtLSAzureContainerInstanceOperator",
    "DbtRunAzureContainerInstanceOperator",
    "DbtRunOperationAzureContainerInstanceOperator",
    "DbtSeedAzureContainerInstanceOperator",
    "DbtSnapshotAzureContainerInstanceOperator",
    "DbtTestAzureContainerInstanceOperator",
    # AWS EKS Execution Mode
    "DbtBuildAwsEksOperator",
    "DbtCloneAwsEksOperator",
    "DbtLSAwsEksOperator",
    "DbtRunAwsEksOperator",
    "DbtRunOperationAwsEksOperator",
    "DbtSeedAwsEksOperator",
    "DbtSnapshotAwsEksOperator",
    "DbtTestAwsEksOperator",
    # GCP Cloud Run Job Execution Mode
    "DbtBuildGcpCloudRunJobOperator",
    "DbtCloneGcpCloudRunJobOperator",
    "DbtLSGcpCloudRunJobOperator",
    "DbtRunGcpCloudRunJobOperator",
    "DbtRunOperationGcpCloudRunJobOperator",
    "DbtSeedGcpCloudRunJobOperator",
    "DbtSnapshotGcpCloudRunJobOperator",
    "DbtTestGcpCloudRunJobOperator",
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
                        "version_deprecated": "1.6.0a1",
                        "deprecation_reason": "`propagate_logs` is no longer necessary as of Cosmos 1.6.0"
                        " because the issue this option was meant to address is no longer an"
                        " issue with Cosmos's new logging approach.",
                        "type": "boolean",
                        "example": None,
                        "default": "True",
                    },
                },
            },
        },
    }
