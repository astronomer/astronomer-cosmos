dbt
=====================

Cosmos provides a set of classes and operators to interact with dbt.

.. currentmodule:: cosmos.providers.dbt

DAGs and Task Groups
---------------------

.. autosummary::
    :toctree: _generated/
    :caption: DAGs and Task Groups

    DbtDag
    DbtTaskGroup


Operators
---------------------

.. autosummary::
    :toctree: _generated/
    :caption: Operators

    DbtRunLocalOperator
    DbtTestLocalOperator
    DbtLSLocalOperator
    DbtSeedLocalOperator
    DbtSnapshotLocalOperator
    DbtRunOperationLocalOperator
    DbtDepsLocalOperator

    DbtLSKubernetesOperator
    DbtRunKubernetesOperator
    DbtRunOperationKubernetesOperator
    DbtSeedKubernetesOperator
    DbtTestKubernetesOperator
    DbtSnapshotKubernetesOperator

    DbtLSDockerOperator
    DbtRunDockerOperator
    DbtRunOperationDockerOperator
    DbtSeedDockerOperator
    DbtTestDockerOperator
    DbtSnapshotDockerOperator
