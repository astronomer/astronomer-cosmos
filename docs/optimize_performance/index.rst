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
   memory_optimization
   caching


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

Caching
~~~~~~~