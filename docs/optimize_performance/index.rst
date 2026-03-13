.. _optimize-performance:

Optimize Cosmos performance
===========================

.. toctree::
   :maxdepth: 0
   :hidden:

   self

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Optimize Performance

   optimize_setup
   optimize_rendering
   Memory optimization <memory_optimization>
   caching
   invocation_mode


Improve your Cosmos project setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cosmos provides a variety of options for how you want to set up dbt in relation to your Airflow environment.
How you first set up your Cosmos project and whether you can run dbt in the Airflow worker or triggerer nodes impacts how you install dbt dependencies, set up connections to your data warehouse, and manage virtual environments.

For guidance about the different setup options and opportunities for performance improvements, see :ref:`optimize_setup`.

Optimize rendering
~~~~~~~~~~~~~~~~~~

Cosmos must parse your dbt project to map it into corresponding Airflow tasks, and to create tasks in a Dag. Depending on how you set up your configurations, Cosmos might parse all of your dbt project every time, instead of parsing only changed files.

See :ref:`optimize_rendering`.

Memory optimization
~~~~~~~~~~~~~~~~~~~

Sometimes, running dbt pipelines with Cosmos can require high memory resources. If you experience Out of Memory (OOM) errors or zombie Airflow tasks during high-memory scenarios, you can make changes to your configuration options and execution modes to improve performance in :ref:`memory_optimization`.

Caching
~~~~~~~

Cosmos can cache many artifacts, like the output of ``dbt ls`` or dbt profiles generated from Airflow connections, to speed up parsing and execution. See :ref:`caching`.
