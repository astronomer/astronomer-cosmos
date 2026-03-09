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

Optimize rendering
~~~~~~~~~~~~~~~~~~

Cosmos must parse your dbt project to map your dbt code into Airflow tasks and to create tasks in a Dag. Depending on how you set up your configurations, Cosmos might parse all of your dbt project every time, instead of parsing only changed files.

See :ref:`optimize_rendering`.


Memory optimization
~~~~~~~~~~~~~~~~~~~

Caching
~~~~~~~