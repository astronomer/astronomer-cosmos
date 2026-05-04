.. _optimize-performance:

Optimize Performance
--------------------

Cosmos performance can be tuned across two dimensions: how fast Dags are parsed (affecting how quickly they appear
and update in `Apache Airflow® <https://airflow.apache.org/>`_) and how fast tasks execute (affecting Dag run duration).

- :ref:`optimize-rendering` -- Speed up Dag parsing by choosing the right LoadMode, reducing Dag granularity, and skipping stale sources.
- :ref:`optimize-execution` -- Speed up Dag runs by choosing the right execution mode, sizing workers, and reducing per-task overhead.
- :ref:`perf-troubleshooting` -- Diagnose common performance issues such as slow parsing, missing Dags, and Out of Memory (OOM) errors.

The following pages cover specific optimization mechanisms in more detail:

- :ref:`memory-optimization` -- Reduce memory consumption during Dag parsing and task execution.
- :ref:`caching` -- How Cosmos caches dbt ls output, partial parse files, profiles, and YAML selectors.
- :ref:`invocation-mode` -- Choose between running dbt as a library or as a subprocess.

.. toctree::
   :maxdepth: 1
   :caption: Optimize Performance

   optimize_rendering
   optimize_execution
   troubleshooting
   memory_optimization
   caching
   invocation_mode
