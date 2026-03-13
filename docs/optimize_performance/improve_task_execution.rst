.. _improve_task_execution:

Improve task execution
=======================

The dbt project that you use and the Cosmos configurations you choose directly affect how well your tasks execute
because both of these factors determine your resource needs.

Track task memory utilization with debug mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are experiencing performance issues during task execution related to memory resource issues, and you can track memory use during tasks by entering debug mode using :ref:`enable_debug_mode`.
When enabled, Cosmos creates an XCom entry per task, named ``cosmos_debug_max_memory_mb``. This contains the maximum amount of RSS memory tracked by the Airflow operator. Memory tracking is polled by default at a ``0.5`` seconds frequency.

1. Enable debug mode by configuring the following, in either the Airflow Config or with environment vairables:

.. code-block:: bash

   # In airflow.cfg
   [cosmos]
   enable_debug_mode = True

.. code-block:: bash

    # Or via environment variable
    export AIRFLOW__COSMOS__ENABLE_DEBUG_MODE=True

2. (Optional) If you want to change the polling rate from the default ``0.5s`` to a different rate, you can use ``AIRFLOW__COSMOS__DEBUG_MEMORY_POLL_INTERVAL_SECONDS``. The following example shows the CLI commands to set it to ``2`` seconds:

.. code-block:: bash

   # In airflow.cfg this decreases the poll interval to 0.25 seconds
   [cosmos]
   debug_memory_poll_interval_seconds = 0.25

.. code-block:: bash

   # Or via environment variable. This increases the poll interval to 2 seconds
   export AIRFLOW__COSMOS__DEBUG_MEMORY_POLL_INTERVAL_SECONDS=2


Reduce task execution time
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to the optimizing your :ref:`Dag parsing <parsing-methods>`, you can reduce task execution time by making the following changes to your Cosmos project.

1. Use the :ref:`watcher-execution-mode`.
2. (BigQuery only) Use the :ref:`async-execution-mode`.
3. Use the :ref:`local-execution`.
4. Disable running ``dbt deps``in Cosmos and Run your ``dbt deps`` as part of the CI.
5. Optimize dbt Core by using dbt as a library instead of a binary with :ref:`how-to-run-dbt-ls`. Install dbt and its adapters in the ``requirements.txt``. This can potentially cause :ref:`dependency conflicts <execution-modes-local-conflicts>` for some setups.
6. For non-critical parts of the pipeline, consider grouping tasks by instantiating :ref:`Cosmos operators <operators>`.
7. Try to use :ref:`dbt Fusion with Cosmos <dbt_fusion>`.


Astro worker node configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you use Cosmos on `Astro <https://www.astronomer.io/>`_, there are Astro-specific configurations that can improve your task execution performance.

Astro Deployments are configured, by default, to handle IO-intensive tasks and not CPU-intensive tasks. ``A5`` instances have five concurrent processes for a single CPU, and this same ratio applies to all Astro worker types.
This ratio of 5:1 concurrent processes per vCPU can cause performance issues with the default Cosmos setup. By default, each Cosmos task both attempts to parse the dbt project and spawns a subprocess to run the dbt command itself, which means 10 processes competing for each vCPU.

Worker type configuration recommendations based on execution:

- Ratio of 2:1 (concurrent processes by vCPU) if you use :ref:`ExecutionMode.LOCAL <local-execution>` with dbt-core and dbt installed in the same Python virtualenv, where you configured the virtualenv in your ``requirements.txt``.
- Ratio of 1:1 (concurrent processes by vCPU)  if using :ref:`ExecutionMode.LOCAL <local-execution>` with dbt-core installed in a separate Python virtualenv, where you configured a dedicated ``venv`` in the Dockerifle to install dbt.
- Ratio of 4:1 (concurrent processes by vCPU) if using :ref:`ExecutionMode.AIRFLOW_ASYNC with <async-execution-mode>` BQ

Change your load mode
+++++++++++++++++++++

Consider using :ref:`LoadMode.MANIFEST <dbt_manifest_load_mode>` instead of :ref:`LoadMode.DBT_LS <dbt_ls_parsing-method>` if you are continue having performance issues. If you must use ``LoadMode.DBT_LS``, the recommended concurrencies in the previous section might need to be reduced further.
