.. _troubleshoot-cosmos:

Troubleshoot Cosmos
============================

I can't see my Dags
~~~~~~~~~~~~~~~~~~~~

Check to see if your ``Dag`` files contain the words ``Dag`` and ``Airflow``.

I still cannot see my DbtDags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you can't see your Dags, then there might be a timeout error occurring as Cosmos tries to parse the dbt project.

1. Check the Dag processor and scheduler logs for parsing times or errors.

Cosmos logs an ``INFO`` message about the amount of time it takes to parse the dbt project. Depending on the duration and load mode, the message look like the following:

    .. code-block::

        It took 0.0684s to parse the dbt project for DAG using LoadMode.DBT_LS_CACHE


2. If you see ``DAGBAG TIMEOUT ERROR`` or, if you're using ``LoadMode.DBT_LS``, increase the ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT``.
You can confirm the Dag parsing time by searching the logs for the ``INFO`` messages to approximate what the timeout length should be, or
you can also use the following resources about how Airflow parses Dags to determine by how much you want to adjust Airflow Dag parsing behavior.

The Airflow Dag Processor, from a high level, applies the following steps while parsing a Dag. You can adjust the default values of these configurations in order to prevent timeout errors:

.. list-table:: Airflow 3 Dag parsing process and configurations
   :widths: 60 20 20
   :header-rows: 1

   * - Parsing Step
     - Configuration
     - Default
   * - 1. The Dag processor checks for new DAG files every 5 minutes.
     - `refresh_interval <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#refresh-interval>`_
     - ``300`` seconds
   * - 2. Reparses Dag files that were not reparsed in the last 30 seconds.
     - `min_file_process_interval <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#min-file-process-interval>`_
     - ``30`` seconds
   * - 3. Parses Dag files via two dedicated processes.
     - `parsing_processes <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#config-dag-processor-parsing-processes>`_
     - ``2`` processes
   * - 4. Each parsing process must be completed within 50s.
     - `dag_file_processor_timeout <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dag-file-processor-timeout>`_
     - ``50`` seconds
   * - 5. Each Python file import must happen within 30s.
     - `dagbag_import_timeout <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dagbag-import-timeout>`__
     - ``30`` seconds


The performance is suboptimal due to latency or resource utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Try using the latest Cosmos release.
2. Leverage Cosmos :ref:`caching` mechanisms.
3. For very large dbt pipelines, use recommended ``LoadMode.DBT_MANIFEST``. See :ref:`optimize_rendering` for more information about improving project rendering.
4. Pre-install dbt deps in your Airflow environment. See :ref:`pre-install-dbt-deps`.
5. If possible, use :ref:`watcher-execution-mode`. See :ref:`improve-execution-mode-dag-run` for more information about how execution modes can affect performance, or :ref:`execution-modes` to select an execution mode that best fits your project needs.
6. Assess your Airflow tasks to see if you can :ref:`improve_task_execution`.


I'm getting out of memory (OOM) errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`memory_optimization`.
