.. _troubleshoot-cosmos:

Troubleshoot Cosmos
============================

I cannot see my DbtDag
~~~~~~~~~~~~~~~~~~~~~~

1. Check to see if your Dag files contain the words ``Dag`` and ``Airflow``.
2. Set the Airflow environment variable, ``AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=False``.

I still cannot see my DbtDags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Find if you are using ``LoadMode.AUTOMATIC`` (default) or ``LoadMode.DBT_LS``.
2. Increase the ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT``.
3. Check the Dag processor/ scheduler logs for errors.

The performance is suboptimal due to latency or resource utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Try using the latest Cosmos release.
2. Leverage Cosmos `caching <../../optimize_performance/caching.html>`_ mechanisms.
3. For very large dbt pipelines, use recommended ``LoadMode.DBT_MANIFEST``.
4. Pre-install dbt deps in your Airflow environment.
5. If possible, use ``ExecutionMode.LOCAL`` or ``InvocationMode.DBT_RUNNER``.

I'm getting out of memory (OOM) errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`memory_optimization`.
