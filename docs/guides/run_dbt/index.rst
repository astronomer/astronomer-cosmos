.. _how-cosmos-runs-dbt:

How Cosmos runs dbt
===================

After the dbt project has been parsed and turned into an Airflow dag or task group, you can execute it.

Cosmos’ ``DbtDag`` and ``DbtTaskGroup`` follow the Airflow best practice of creating tasks that are as atomic as
possible, meaning that there is a separate task for each dbt command. This leads to improved visibility and the
possibility of fine-grained control over your dbt commands. For example, you can set task parameters like pool
or retries, on individual Cosmos tasks. Or, you can make downstream tasks run as soon as a specific Cosmos task has
finished successfully.

Cosmos uses different kinds of configurations to control how the dbt commands are executed within the Airflow dag or task group.

Execution modes
~~~~~~~~~~~~~~~~

Execution modes are defined by the ``ExecutionConfig`` class in your Cosmos Dag.
Depending on your specific dbt project architecture and whether you want to run your dbt commands in the cloud or in a container separate from your Airflow environment.

Check out the available :ref:`execution-modes` and the detailed :ref:`execution-config` for more information about how to set up your Cosmos execution.


Running dbt commands
~~~~~~~~~~~~~~~~~~~~

After specifying where you want Cosmos to run dbt commands, you can additionally specify the commands you want to run, how you want


Running Airflow
~~~~~~~~~~~~~~~