# type: ignore # ignores "Cannot assign to a type" MyPy error

"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""

from __future__ import annotations

import importlib

__version__ = "1.14.0a6"


# Mapping of public names to their module paths for lazy loading via __getattr__.
# Imports are deferred until first access, which avoids circular imports during
# Airflow >= 3.2 provider discovery (where airflow.configuration is not yet
# initialized) and reduces memory footprint by only loading what is actually used.
_LAZY_IMPORTS: dict[str, str] = {
    "DbtDag": "cosmos.airflow.dag",
    "DbtTaskGroup": "cosmos.airflow.task_group",
    "ExecutionConfig": "cosmos.config",
    "ProfileConfig": "cosmos.config",
    "ProjectConfig": "cosmos.config",
    "RenderConfig": "cosmos.config",
    "DbtResourceType": "cosmos.constants",
    "ExecutionMode": "cosmos.constants",
    "InvocationMode": "cosmos.constants",
    "LoadMode": "cosmos.constants",
    "SourceRenderingBehavior": "cosmos.constants",
    "TestBehavior": "cosmos.constants",
    "TestIndirectSelection": "cosmos.constants",
    "MissingPackage": "cosmos.operators.lazy_load",
    # Local
    "DbtBuildLocalOperator": "cosmos.operators.local",
    "DbtCloneLocalOperator": "cosmos.operators.local",
    "DbtDepsLocalOperator": "cosmos.operators.local",
    "DbtLSLocalOperator": "cosmos.operators.local",
    "DbtRunLocalOperator": "cosmos.operators.local",
    "DbtRunOperationLocalOperator": "cosmos.operators.local",
    "DbtSeedLocalOperator": "cosmos.operators.local",
    "DbtSnapshotLocalOperator": "cosmos.operators.local",
    "DbtTestLocalOperator": "cosmos.operators.local",
    # Docker
    "DbtBuildDockerOperator": "cosmos.operators.docker",
    "DbtCloneDockerOperator": "cosmos.operators.docker",
    "DbtLSDockerOperator": "cosmos.operators.docker",
    "DbtRunDockerOperator": "cosmos.operators.docker",
    "DbtRunOperationDockerOperator": "cosmos.operators.docker",
    "DbtSeedDockerOperator": "cosmos.operators.docker",
    "DbtSnapshotDockerOperator": "cosmos.operators.docker",
    "DbtTestDockerOperator": "cosmos.operators.docker",
    # Kubernetes
    "DbtBuildKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtCloneKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtLSKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtRunKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtRunOperationKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtSeedKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtSnapshotKubernetesOperator": "cosmos.operators.kubernetes",
    "DbtTestKubernetesOperator": "cosmos.operators.kubernetes",
    # Azure Container Instance
    "DbtBuildAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtCloneAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtLSAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtRunAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtRunOperationAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtSeedAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtSnapshotAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    "DbtTestAzureContainerInstanceOperator": "cosmos.operators.azure_container_instance",
    # AWS EKS
    "DbtBuildAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtCloneAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtLSAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtRunAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtRunOperationAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtSeedAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtSnapshotAwsEksOperator": "cosmos.operators.aws_eks",
    "DbtTestAwsEksOperator": "cosmos.operators.aws_eks",
    # AWS ECS
    "DbtBuildAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtLSAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtRunAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtRunOperationAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtSeedAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtSnapshotAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtTestAwsEcsOperator": "cosmos.operators.aws_ecs",
    "DbtSourceAwsEcsOperator": "cosmos.operators.aws_ecs",
    # GCP Cloud Run Job
    "DbtBuildGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtCloneGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtLSGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtRunGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtRunOperationGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtSeedGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtSnapshotGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
    "DbtTestGcpCloudRunJobOperator": "cosmos.operators.gcp_cloud_run_job",
}


_OPTIONAL_DEPS: dict[str, str] = {
    "cosmos.operators.docker": "docker",
    "cosmos.operators.kubernetes": "kubernetes",
    "cosmos.operators.azure_container_instance": "azure-container-instance",
    "cosmos.operators.aws_eks": "aws_eks",
    "cosmos.operators.aws_ecs": "aws-ecs",
    "cosmos.operators.gcp_cloud_run_job": "gcp-cloud-run-job",
}


def __getattr__(name: str) -> object:
    """Lazy import: resolve public names on first access."""
    if name in _LAZY_IMPORTS:
        module_path = _LAZY_IMPORTS[name]
        try:
            module = importlib.import_module(module_path)
            value = getattr(module, name)
        except (ImportError, AttributeError):
            if module_path in _OPTIONAL_DEPS:
                from cosmos.operators.lazy_load import MissingPackage

                value = MissingPackage(f"{module_path}.{name}", _OPTIONAL_DEPS[module_path])
            else:
                raise
        globals()[name] = value
        return value

    # Submodule access (e.g. cosmos.settings)
    try:
        return importlib.import_module(f"{__name__}.{name}")
    except ModuleNotFoundError as exc:
        # Only convert to AttributeError when the *requested* submodule does not
        # exist.  If the submodule exists but has an internal ImportError we must
        # let it propagate so the real root cause is visible to the user.
        if exc.name == f"{__name__}.{name}":
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
        raise


__all__ = [
    "__version__",
    *_LAZY_IMPORTS,
]
