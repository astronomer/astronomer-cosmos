"""
dbt support for Airflow. Contains dags, task groups, and operators.
"""

from .core.operators.docker import (
    DbtLSDockerOperator,
    DbtRunDockerOperator,
    DbtRunOperationDockerOperator,
    DbtSeedDockerOperator,
    DbtTestDockerOperator,
    DbtSnapshotDockerOperator,
)
from .core.operators.kubernetes import (
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
    DbtSnapshotKubernetesOperator,
)

# re-export the operators
from .core.operators.local import (
    DbtDepsLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtTestLocalOperator,
    DbtSnapshotLocalOperator,
)

# re-export user facing utilities
from cosmos.providers.dbt.core.utils.data_aware_scheduling import get_dbt_dataset

# re-export the dag and task group
from cosmos.providers.dbt.dag import DbtDag
from cosmos.providers.dbt.task_group import DbtTaskGroup

__all__ = [
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    DbtTestLocalOperator,
    DbtDepsLocalOperator,
    DbtSnapshotLocalOperator,
    DbtLSDockerOperator,
    DbtRunOperationDockerOperator,
    DbtRunDockerOperator,
    DbtSeedDockerOperator,
    DbtTestDockerOperator,
    DbtSnapshotDockerOperator,
    DbtLSKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
    DbtSnapshotKubernetesOperator,
    get_dbt_dataset,
    DbtDag,
    DbtTaskGroup,
]
