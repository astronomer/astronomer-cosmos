.. _troubleshoot-cosmos:

Troubleshoot Cosmos
============================

I still cannot see my DbtDags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Increase the ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT``.
2. Check the Dag processor and scheduler logs for errors
3. Find if you are using ``LoadMode.AUTOMATIC`` (default) or ``LoadMode.DBT_LS``.

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
