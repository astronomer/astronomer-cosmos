.. _how-cosmos-runs-dbt:

How Cosmos runs dbt
-------------------

Cosmos can run dbt commands directly using operators, or, after the dbt project has been parsed and turned into an `Apache Airflow® <https://airflow.apache.org/>`_ Dag or task group, you can execute it.

In many execution modes, Cosmos ``DbtDag`` and ``DbtTaskGroup`` create a separate task for each dbt node (model, seed, snapshot).
This leads to improved visibility and the
possibility of fine-grained control over your dbt commands. For example, you can set task parameters like pool
or retries on individual Cosmos tasks. Or, you can make downstream tasks run as soon as a specific Cosmos task has finished successfully.
Running one dbt command per task can bring performance challenges, since each invocation of a dbt command incurs overhead. To improve performance, newer versions of Cosmos have introduced alternatives that offer the same level of granularity while centralising the execution of the dbt command in a single task. Check :ref:`watcher-execution-mode` and :ref:`async-execution-mode`, for more information.

Cosmos uses different kinds of configurations to control how the dbt nodes are executed within the Airflow Dag or task group, which you can customize based on your project and needs.

Execution modes
+++++++++++++++

Execution modes are defined by the ``ExecutionConfig`` class in your Cosmos Dag.
Depending on your specific dbt project architecture and whether you want to run your dbt commands in the cloud or in a container separate from your Airflow environment.

Check out the available :ref:`execution-modes` and the detailed :ref:`execution-config` for more information about how to set up your Cosmos execution.


Running dbt commands
++++++++++++++++++++

In addition to specifying where you want Cosmos to run dbt commands, you can also configure the following:

- :ref:`callbacks`: Tell Cosmos how to handle artifacts produced by dbt while executing dbt code.
- ``interceptor``: (new in v1.14) Optional list of callables run before building the dbt command. See :ref:`operator-args` or for more information.
- :ref:`operator-args`: Pass specific operator arguments, ``operator_args``, in your Dag that can directly correspond to dbt commands, Cosmos operations, or to define Airflow behavior.
- :ref:`scheduling`: Leverage Airflow to schedule your dbt workflows with cron-based scheduling, timetables, and data-aware scheduling.
- :ref:`partial-parsing`: Configure Cosmos to use dbt's partial parsing capabilities, improving dbt and Dag parsing, which speeds up execution times.
