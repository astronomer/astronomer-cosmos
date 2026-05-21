.. _invocation-mode:

Invocation modes
================
.. versionadded:: 1.4

For ``ExecutionMode.LOCAL`` execution mode, Cosmos supports two invocation modes for running dbt:

1. ``InvocationMode.SUBPROCESS``: In this mode, Cosmos runs dbt cli commands using the Python ``subprocess`` module and parses the output to capture logs and to raise exceptions.

2. ``InvocationMode.DBT_RUNNER``: In this mode, Cosmos uses the ``dbtRunner`` available for `dbt programmatic invocations <https://docs.getdbt.com/reference/programmatic-invocations>`__ to run dbt commands. \
   In order to use this mode, dbt must be installed in the same local environment. This mode does not have the overhead of spawning new subprocesses or parsing the output of dbt commands and is faster than ``InvocationMode.SUBPROCESS``. \
   This mode requires dbt version 1.5.0 or higher. It is up to the user to resolve :ref:`execution-modes-local-conflicts` when using this mode.

.. note::

   When ``InvocationMode.DBT_RUNNER`` is used with dbt-core 1.5.6 or later,
   Cosmos automatically appends ``--no-static-parser`` to every dbt command
   it invokes. Cosmos copies the dbt project into a temporary directory at
   both DAG parse time and task execution time, and the differing temp paths
   can interfere with dbt's static parser and cause task hangs. Disabling the
   static parser is a workaround for this interaction. If you rely on dbt's
   static parser, use ``InvocationMode.SUBPROCESS`` instead.

The invocation mode can be set in the ``ExecutionConfig`` as shown below:

.. code-block:: python

    from cosmos.constants import InvocationMode

    dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.DBT_RUNNER,
        ),
    )

If the invocation mode is not set, Cosmos will attempt to use ``InvocationMode.DBT_RUNNER`` if dbt is installed in the same environment as the worker, otherwise it will fall back to ``InvocationMode.SUBPROCESS``.
