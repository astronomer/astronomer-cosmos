"""
dbt support for Airflow. Contains dags, task groups, and operators.
"""

from .core.operators.docker import (
    DbtLSDockerOperator,
    DbtRunDockerOperator,
    DbtRunOperationDockerOperator,
    DbtSeedDockerOperator,
    DbtTestDockerOperator,
)
from .core.operators.kubernetes import (
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
)

# re-export the operators
from .core.operators.local import (
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtTestLocalOperator,
)

# re-export user facing utilities
from .core.utils.data_aware_scheduling import get_dbt_dataset

# re-export the dag and task group
from .dag import DbtDag
from .task_group import DbtTaskGroup

__all__ = [
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    DbtTestLocalOperator,
    DbtLSDockerOperator,
    DbtRunOperationDockerOperator,
    DbtRunDockerOperator,
    DbtSeedDockerOperator,
    DbtTestDockerOperator,
    DbtLSKubernetesOperator,
    DbtRunOperationKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
    get_dbt_dataset,
    DbtDag,
    DbtTaskGroup,
]
