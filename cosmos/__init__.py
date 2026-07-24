"""
Astronomer Cosmos is a library for rendering dbt workflows in Airflow.

Contains dags, task groups, and operators.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__version__ = "1.15.1a2"


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
    "SeedRenderingBehavior": "cosmos.constants",
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

# TYPE_CHECKING block gives static type checkers (mypy, Pylance, pyright) the
# concrete types they need. At runtime TYPE_CHECKING is False, so nothing is
# imported eagerly here — __getattr__ below still handles lazy loading.
# The `import X as X` form is the PEP 484 / ruff-recognised pattern for
# intentional re-exports; it suppresses F401 "imported but unused" warnings.
if TYPE_CHECKING:
    from cosmos.airflow.dag import DbtDag as DbtDag
    from cosmos.airflow.task_group import DbtTaskGroup as DbtTaskGroup
    from cosmos.config import ExecutionConfig as ExecutionConfig
    from cosmos.config import ProfileConfig as ProfileConfig
    from cosmos.config import ProjectConfig as ProjectConfig
    from cosmos.config import RenderConfig as RenderConfig
    from cosmos.constants import DbtResourceType as DbtResourceType
    from cosmos.constants import ExecutionMode as ExecutionMode
    from cosmos.constants import InvocationMode as InvocationMode
    from cosmos.constants import LoadMode as LoadMode
    from cosmos.constants import SeedRenderingBehavior as SeedRenderingBehavior
    from cosmos.constants import SourceRenderingBehavior as SourceRenderingBehavior
    from cosmos.constants import TestBehavior as TestBehavior
    from cosmos.constants import TestIndirectSelection as TestIndirectSelection
    from cosmos.operators.aws_ecs import DbtBuildAwsEcsOperator as DbtBuildAwsEcsOperator
    from cosmos.operators.aws_ecs import DbtLSAwsEcsOperator as DbtLSAwsEcsOperator
    from cosmos.operators.aws_ecs import DbtRunAwsEcsOperator as DbtRunAwsEcsOperator
    from cosmos.operators.aws_ecs import (
        DbtRunOperationAwsEcsOperator as DbtRunOperationAwsEcsOperator,
    )
    from cosmos.operators.aws_ecs import DbtSeedAwsEcsOperator as DbtSeedAwsEcsOperator
    from cosmos.operators.aws_ecs import DbtSnapshotAwsEcsOperator as DbtSnapshotAwsEcsOperator
    from cosmos.operators.aws_ecs import DbtSourceAwsEcsOperator as DbtSourceAwsEcsOperator
    from cosmos.operators.aws_ecs import DbtTestAwsEcsOperator as DbtTestAwsEcsOperator
    from cosmos.operators.aws_eks import DbtBuildAwsEksOperator as DbtBuildAwsEksOperator
    from cosmos.operators.aws_eks import DbtCloneAwsEksOperator as DbtCloneAwsEksOperator
    from cosmos.operators.aws_eks import DbtLSAwsEksOperator as DbtLSAwsEksOperator
    from cosmos.operators.aws_eks import DbtRunAwsEksOperator as DbtRunAwsEksOperator
    from cosmos.operators.aws_eks import (
        DbtRunOperationAwsEksOperator as DbtRunOperationAwsEksOperator,
    )
    from cosmos.operators.aws_eks import DbtSeedAwsEksOperator as DbtSeedAwsEksOperator
    from cosmos.operators.aws_eks import DbtSnapshotAwsEksOperator as DbtSnapshotAwsEksOperator
    from cosmos.operators.aws_eks import DbtTestAwsEksOperator as DbtTestAwsEksOperator
    from cosmos.operators.azure_container_instance import (
        DbtBuildAzureContainerInstanceOperator as DbtBuildAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtCloneAzureContainerInstanceOperator as DbtCloneAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtLSAzureContainerInstanceOperator as DbtLSAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtRunAzureContainerInstanceOperator as DbtRunAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtRunOperationAzureContainerInstanceOperator as DbtRunOperationAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtSeedAzureContainerInstanceOperator as DbtSeedAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtSnapshotAzureContainerInstanceOperator as DbtSnapshotAzureContainerInstanceOperator,
    )
    from cosmos.operators.azure_container_instance import (
        DbtTestAzureContainerInstanceOperator as DbtTestAzureContainerInstanceOperator,
    )
    from cosmos.operators.docker import DbtBuildDockerOperator as DbtBuildDockerOperator
    from cosmos.operators.docker import DbtCloneDockerOperator as DbtCloneDockerOperator
    from cosmos.operators.docker import DbtLSDockerOperator as DbtLSDockerOperator
    from cosmos.operators.docker import DbtRunDockerOperator as DbtRunDockerOperator
    from cosmos.operators.docker import (
        DbtRunOperationDockerOperator as DbtRunOperationDockerOperator,
    )
    from cosmos.operators.docker import DbtSeedDockerOperator as DbtSeedDockerOperator
    from cosmos.operators.docker import DbtSnapshotDockerOperator as DbtSnapshotDockerOperator
    from cosmos.operators.docker import DbtTestDockerOperator as DbtTestDockerOperator
    from cosmos.operators.gcp_cloud_run_job import (
        DbtBuildGcpCloudRunJobOperator as DbtBuildGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtCloneGcpCloudRunJobOperator as DbtCloneGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtLSGcpCloudRunJobOperator as DbtLSGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtRunGcpCloudRunJobOperator as DbtRunGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtRunOperationGcpCloudRunJobOperator as DbtRunOperationGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtSeedGcpCloudRunJobOperator as DbtSeedGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtSnapshotGcpCloudRunJobOperator as DbtSnapshotGcpCloudRunJobOperator,
    )
    from cosmos.operators.gcp_cloud_run_job import (
        DbtTestGcpCloudRunJobOperator as DbtTestGcpCloudRunJobOperator,
    )
    from cosmos.operators.kubernetes import (
        DbtBuildKubernetesOperator as DbtBuildKubernetesOperator,
    )
    from cosmos.operators.kubernetes import (
        DbtCloneKubernetesOperator as DbtCloneKubernetesOperator,
    )
    from cosmos.operators.kubernetes import DbtLSKubernetesOperator as DbtLSKubernetesOperator
    from cosmos.operators.kubernetes import DbtRunKubernetesOperator as DbtRunKubernetesOperator
    from cosmos.operators.kubernetes import (
        DbtRunOperationKubernetesOperator as DbtRunOperationKubernetesOperator,
    )
    from cosmos.operators.kubernetes import (
        DbtSeedKubernetesOperator as DbtSeedKubernetesOperator,
    )
    from cosmos.operators.kubernetes import (
        DbtSnapshotKubernetesOperator as DbtSnapshotKubernetesOperator,
    )
    from cosmos.operators.kubernetes import (
        DbtTestKubernetesOperator as DbtTestKubernetesOperator,
    )
    from cosmos.operators.lazy_load import MissingPackage as MissingPackage
    from cosmos.operators.local import DbtBuildLocalOperator as DbtBuildLocalOperator
    from cosmos.operators.local import DbtCloneLocalOperator as DbtCloneLocalOperator
    from cosmos.operators.local import DbtDepsLocalOperator as DbtDepsLocalOperator
    from cosmos.operators.local import DbtLSLocalOperator as DbtLSLocalOperator
    from cosmos.operators.local import DbtRunLocalOperator as DbtRunLocalOperator
    from cosmos.operators.local import DbtRunOperationLocalOperator as DbtRunOperationLocalOperator
    from cosmos.operators.local import DbtSeedLocalOperator as DbtSeedLocalOperator
    from cosmos.operators.local import DbtSnapshotLocalOperator as DbtSnapshotLocalOperator
    from cosmos.operators.local import DbtTestLocalOperator as DbtTestLocalOperator


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
