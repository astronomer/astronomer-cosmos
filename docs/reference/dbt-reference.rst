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
