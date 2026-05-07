.. _operators:

dbt command operators
---------------------

Cosmos exposes individual operators that correspond to specific dbt commands, which can be used just like traditional
`Apache Airflow® <https://airflow.apache.org/>`_ operators. Cosmos names these operators using the format ``Dbt<dbt-command><execution-mode>Operator``.

You can see the full example DAG in the `dev/dags directory <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/example_operators.py>`_.

Run
+++

Requires Cosmos >= 0.6.0.

The ``DbtRunLocalOperator`` implements the `dbt run <https://docs.getdbt.com/reference/commands/run>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START run_local_example]
    :end-before: [END run_local_example]


Test
++++

Requires Cosmos >= 0.6.0.

The ``DbtTestLocalOperator`` implements the `dbt test <https://docs.getdbt.com/reference/commands/test>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START test_local_example]
    :end-before: [END test_local_example]


Snapshot
++++++++

Requires Cosmos >= 0.6.0.

The ``DbtSnapshotLocalOperator`` implements the `dbt snapshot <https://docs.getdbt.com/reference/commands/snapshot>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START snapshot_local_example]
    :end-before: [END snapshot_local_example]


Build
+++++

Requires Cosmos >= 1.4.0.

The ``DbtBuildLocalOperator`` implements the `dbt build <https://docs.getdbt.com/reference/commands/build>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START build_local_example]
    :end-before: [END build_local_example]


Seed
++++

Requires Cosmos >= 0.6.0.

The ``DbtSeedLocalOperator`` implements the `dbt seed <https://docs.getdbt.com/reference/commands/seed>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START seed_local_example]
    :end-before: [END seed_local_example]


Clone
+++++

Requires Cosmos >= 1.8.0.

The ``DbtCloneLocalOperator`` implements the `dbt clone <https://docs.getdbt.com/reference/commands/clone>`_ command.

.. literalinclude:: ../../../../dev/dags/example_operators.py
    :language: python
    :start-after: [START clone_example]
    :end-before: [END clone_example]
