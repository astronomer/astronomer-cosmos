.. _optimize-execution:

Optimize Task Execution
-----------------------

Once your DAG is parsed, performance depends on how quickly tasks execute. Each Cosmos task runs one or more dbt
commands, and the overhead of those invocations adds up across a DAG run. This page covers the most impactful ways
to reduce DAG run time.


1. Use an efficient execution mode
+++++++++++++++++++++++++++++++++++

The execution mode determines how Cosmos runs dbt commands at task execution time. Choosing the right mode is the
single most impactful change for DAG run performance.

**Recommended: use** ``ExecutionMode.WATCHER``

In the default ``ExecutionMode.LOCAL``, every model runs as a separate ``dbt run`` invocation, which introduces
per-task overhead. ``ExecutionMode.WATCHER`` runs a single ``dbt build`` across the entire project and uses
dbt's native threading to parallelize models, while still giving you model-level visibility in `Apache Airflow® <https://airflow.apache.org/>`_.

Benchmarks show **up to 80% reduction in DAG run time** compared to ``ExecutionMode.LOCAL``.
See :ref:`watcher-execution-mode` for setup instructions and detailed benchmarks.

.. note::

   ``ExecutionMode.WATCHER`` is currently experimental. Review its
   `known limitations <https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/airflow-worker/watcher-execution-mode.html#known-limitations>`_
   before adopting it in production.

.. code-block:: python

   from cosmos import DbtDag, ExecutionConfig
   from cosmos.constants import ExecutionMode

   DbtDag(
       dag_id="my_dbt_dag",
       execution_config=ExecutionConfig(
           execution_mode=ExecutionMode.WATCHER,
       ),
       # ...
   )

.. tip::

   ``ExecutionMode.WATCHER`` performance scales with dbt ``threads``. Start with a conservative value that matches
   the CPU capacity of your workers, then increase gradually. See
   :ref:`watcher-execution-mode` for how to configure threads.

**Alternative for BigQuery: use** ``ExecutionMode.AIRFLOW_ASYNC``

If you use BigQuery, ``ExecutionMode.AIRFLOW_ASYNC`` pre-compiles SQL transformations and executes them using
Airflow's deferrable ``BigQueryInsertJobOperator``. This frees worker slots while queries execute in BigQuery,
achieving roughly **35% faster execution** compared to ``ExecutionMode.LOCAL``. See :ref:`async-execution-mode`.

**Baseline: use** ``ExecutionMode.LOCAL`` **with** ``InvocationMode.DBT_RUNNER``

If neither ``WATCHER`` nor ``AIRFLOW_ASYNC`` suits your setup, configure ``ExecutionMode.LOCAL`` to use
``InvocationMode.DBT_RUNNER`` to run dbt as a library call rather than spawning a subprocess for each task.
Since Cosmos 1.4, ``DBT_RUNNER`` is the preferred invocation mode and is auto-selected when dbt is available in
the same Python environment; otherwise Cosmos falls back to ``InvocationMode.SUBPROCESS``. See :ref:`invocation-mode`.


2. Install dbt in the same Python environment as Airflow
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

When dbt is installed alongside Airflow, Cosmos uses dbt's programmatic API (``dbtRunner``) instead of spawning
subprocesses. This eliminates process creation overhead and reduces both CPU and memory usage during task execution
and DAG parsing.

This is required for ``InvocationMode.DBT_RUNNER`` and yields the best performance with ``ExecutionMode.WATCHER``.

For more details, see :ref:`invocation-mode`.


3. Pre-install dbt dependencies
+++++++++++++++++++++++++++++++

By default, Cosmos runs ``dbt deps`` during both DAG parsing and task execution to ensure packages are available.
For large projects with many packages, this adds significant overhead to every task.

**Pre-install packages in your Docker image:**

.. code-block:: docker

   # In your Dockerfile
   COPY dbt_project/ /opt/dbt/project/
   RUN cd /opt/dbt/project && dbt deps

**Then disable runtime installation:**

.. code-block:: python

   from cosmos import ProjectConfig

   ProjectConfig(
       dbt_project_path="/opt/dbt/project",
       install_dbt_deps=False,
   )


4. Use profiles.yml instead of profile mapping
+++++++++++++++++++++++++++++++++++++++++++++++

Cosmos can generate dbt profiles at runtime from Airflow connections using profile mapping classes. While convenient,
this adds overhead to each task invocation because Cosmos must read the Airflow connection and construct the profile.

If performance is a priority, provide a ``profiles.yml`` file directly. This avoids the runtime profile generation
entirely.

For how to configure this, see
`Using your profiles.yml <https://astronomer.github.io/astronomer-cosmos/guides/connect_database/use-your-profiles-yml.html>`_.


5. Worker node sizing
+++++++++++++++++++++

The adequate resources needed to run Cosmos tasks depend on your dbt project and the Cosmos configuration chosen.
Airflow workers configured for IO-intensive workloads (the default on Astro, which uses a ratio of 5 concurrent
processes per vCPU) may not have enough CPU capacity for Cosmos tasks, which involve parsing dbt projects and running
dbt commands.

The following table provides recommended concurrency ratios based on execution mode:

.. list-table:: Recommended worker concurrency (concurrent processes per vCPU)
   :header-rows: 1
   :widths: 50 20

   * - Execution mode
     - Ratio
   * - ``ExecutionMode.LOCAL`` with dbt in the same Python environment
     - 2:1
   * - ``ExecutionMode.LOCAL`` with dbt in a separate virtual environment
     - 1:1
   * - ``ExecutionMode.AIRFLOW_ASYNC`` (BigQuery)
     - 4:1

.. note::

   Keep in mind that Airflow re-parses the DAG file on the worker node every time a task runs. If you are using
   ``LoadMode.DBT_LS``, this means each task also triggers a dbt project parse. Consider using
   ``LoadMode.DBT_MANIFEST`` to reduce worker-side parsing overhead. See :ref:`optimize-rendering`.

If you are using ``ExecutionMode.WATCHER``, the producer task is CPU and memory intensive while the consumer sensor
tasks are lightweight. Use the ``watcher_dbt_execution_queue``
`configuration <https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/airflow-worker/watcher-execution-mode.html#watcher-dbt-execution-queue>`_
to route the producer task and sensor retries to a worker queue with more resources.


6. Profile memory usage with debug mode
++++++++++++++++++++++++++++++++++++++++

To right-size your workers, enable Cosmos debug mode to measure actual memory consumption per task:

.. code-block:: bash

   export AIRFLOW__COSMOS__ENABLE_DEBUG_MODE=True

When enabled, Cosmos tracks peak memory usage during task execution and pushes it to XCom under the key
``cosmos_debug_max_memory_mb``. Use this data to:

- Identify which tasks consume the most memory
- Set appropriate memory limits and worker queue assignments
- Detect memory regressions over time

For high-memory tasks, consider using separate
`Airflow pools <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html>`_
or the ``watcher_dbt_execution_queue`` configuration to route them to workers with more resources.

For more memory optimization strategies, see :ref:`memory-optimization`.
