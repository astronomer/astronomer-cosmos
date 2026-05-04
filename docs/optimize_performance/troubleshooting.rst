.. _perf-troubleshooting:

Troubleshooting Performance
---------------------------

This page helps you diagnose common performance issues when running Cosmos.


Measuring DAG parse time
+++++++++++++++++++++++++

Cosmos logs the time it takes to parse each dbt project at the ``INFO`` level. Search your `Apache Airflow® <https://airflow.apache.org/>`_ scheduler or
DAG processor logs for messages like:

.. code-block:: text

   Cosmos performance (my_dbt_dag) - [worker-1|12345]: It took 0.068s to parse the dbt project for DAG using LoadMode.DBT_LS_CACHE

This tells you:

- Which DAG was parsed (``my_dbt_dag``)
- Which node and process performed the parse
- How long parsing took (``0.068s``)
- Which ``LoadMode`` was used (``LoadMode.DBT_LS_CACHE``)

If the parse time is high, see :ref:`optimize-rendering` for strategies to reduce it.


Apache Airflow® DAG processor configuration
++++++++++++++++++++++++++++++++++++++++++++

The Airflow DAG processor controls how often and how quickly DAG files are parsed. Understanding these settings helps
you diagnose issues where DAGs are slow to appear or update.

.. list-table::
   :header-rows: 1
   :widths: 35 30 10 25

   * - What it controls
     - Airflow 3 setting
     - Default
     - Airflow 2 setting
   * - How often new DAG files are detected
     - ``[dag_processor] refresh_interval``
     - 5 min
     - ``[scheduler] dag_dir_list_interval``
   * - Minimum interval between reparses of the same file
     - ``[dag_processor] min_file_process_interval``
     - 30s
     - ``[scheduler] min_file_process_interval``
   * - Number of concurrent parsing processes
     - ``[dag_processor] parsing_processes``
     - 2
     - ``[scheduler] max_threads``
   * - Maximum time to parse a single DAG file
     - ``[dag_processor] dag_file_processor_timeout``
     - 50s
     - ``[core] dag_file_processor_timeout``
   * - Maximum time to import a single Python file
     - ``[dag_processor] dagbag_import_timeout``
     - 30s
     - ``[core] dagbag_import_timeout``

.. note::

   In Airflow 3, DAG processor settings moved from the ``[core]`` and ``[scheduler]`` sections to the
   ``[dag_processor]`` section. Environment variable overrides follow the same pattern:
   ``AIRFLOW__DAG_PROCESSOR__DAGBAG_IMPORT_TIMEOUT`` instead of ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT``.


DAGs not appearing
++++++++++++++++++

If your ``DbtDag`` or ``DbtTaskGroup`` does not appear in Airflow, the most likely cause is that DAG parsing exceeds
the ``dagbag_import_timeout``.

**Diagnosis steps:**

1. Check the Cosmos parse time log (see above). If the time exceeds the ``dagbag_import_timeout`` (default 30s),
   the DAG file will be silently dropped.

2. Check the Airflow DAG processor logs for timeout or import errors related to your DAG file.

**Solutions (in order of preference):**

1. **Reduce the parse time** by following the recommendations in :ref:`optimize-rendering`. The most impactful
   change is switching to ``LoadMode.DBT_MANIFEST``.

2. **Increase the timeout** only if reducing parse time is not sufficient:

   .. code-block:: bash

      # Airflow 3
      export AIRFLOW__DAG_PROCESSOR__DAGBAG_IMPORT_TIMEOUT=120

      # Airflow 2
      export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120


OOM errors during task execution
++++++++++++++++++++++++++++++++

If tasks are being killed by the OS or reaching a zombie state, they are likely running out of memory.

**Diagnosis steps:**

1. Enable Cosmos debug mode to measure peak memory usage per task:

   .. code-block:: bash

      export AIRFLOW__COSMOS__ENABLE_DEBUG_MODE=True

   After tasks run, check the ``cosmos_debug_max_memory_mb`` XCom value to identify memory-hungry tasks.

2. Check if dbt and Airflow are in separate Python environments. Running dbt as a subprocess
   (``InvocationMode.SUBPROCESS``) roughly doubles memory usage compared to running it as a library
   (``InvocationMode.DBT_RUNNER``).

**Solutions:**

- Enable memory-optimized imports to reduce the Cosmos memory footprint:

  .. code-block:: bash

     export AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS=True

  Note: when enabled, use full module paths for imports (e.g., ``from cosmos.airflow.dag import DbtDag`` instead
  of ``from cosmos import DbtDag``). See :ref:`memory-optimization` for details.

- Use ``ExecutionMode.WATCHER`` to run dbt in a single process with threading instead of spawning a separate
  process per model. See :ref:`watcher-execution-mode`.

- Route memory-intensive tasks to workers with more resources using Airflow pools or the
  ``watcher_dbt_execution_queue`` configuration.

For a comprehensive list of memory optimization options, see :ref:`memory-optimization`.
