.. _troubleshoot-cosmos:

Troubleshoot Cosmos
============================

I can't see my Dags
~~~~~~~~~~~~~~~~~~~~

Check to see if your ``Dag`` files contain the words ``Dag`` and ``Airflow``.

I still cannot see my DbtDags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Check the Dag processor and scheduler logs for errors.
2. Find if you are using ``LoadMode.AUTOMATIC`` (default) or ``LoadMode.DBT_LS``. If you see ``DAGBAG TIMEOUT ERROR`` or, if you're using ``LoadMode.DBT_LS``, increase the ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT``.

The performance is suboptimal due to latency or resource utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Try using the latest Cosmos release.
2. Leverage Cosmos :ref:`caching` mechanisms.
3. For very large dbt pipelines, use recommended ``LoadMode.DBT_MANIFEST``.
4. Pre-install dbt deps in your Airflow environment.
5. If possible, use ``ExecutionMode.LOCAL`` or ``InvocationMode.DBT_RUNNER``.

I'm getting out of memory (OOM) errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`memory_optimization`.
