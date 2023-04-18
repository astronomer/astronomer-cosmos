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

__all__ = [
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    DbtTestLocalOperator,
    DbtDepsLocalOperator,
    DbtSnapshotLocalOperator,
    get_dbt_dataset,
    DbtDag,
    DbtTaskGroup,
]
