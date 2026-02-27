.. _operators:

Operators
=========

Cosmos exposes individual operators that correspond to specific dbt commands, which can be used just like traditional
`Apache Airflow® <https://airflow.apache.org/>`_ operators. Cosmos names these operators using the format ``Dbt<dbt-command><execution-mode>Operator``. For example, ``DbtBuildLocalOperator``.

Clone
-----

Requirement

* Cosmos >= 1.8.0
* dbt-core >= 1.6.0

The ``DbtCloneLocalOperator`` implement `dbt clone <https://docs.getdbt.com/reference/commands/clone>`_ command.

Example of how to use

.. literalinclude:: ../../dev/dags/example_operators.py
    :language: python
    :start-after: [START clone_example]
    :end-before: [END clone_example]


Seed
----

The ``DbtSeedLocalOperator`` implements the `dbt seed <https://docs.getdbt.com/reference/commands/seed>`_ command.

In this example, we're using the ``DbtSeedLocalOperator`` to seed ``raw_orders``.

.. literalinclude:: ../../dev/dags/example_operators.py
    :language: python
    :start-after: [START seed_local_example]
    :end-before: [END seed_local_example]
