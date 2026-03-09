Reference
=========

.. toctree::
   :maxdepth: 0
   :hidden:

   self

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Configurations

   configs/index

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Profiles

   profiles/index

There are different configurations and profiles that you can use to configure how Cosmos works.

- `ProjectConfig <configs/project-config.html>`_: The ``ProjectConfig`` contains information about which dbt project a Cosmos Dag or task group is going to execute, as well as configurations that apply to both rendering and execution.
- `ExecutionConfig <configs/execution-config.html>`_: The ``ExecutionConfig`` determines where and how the dbt commands are run within Cosmos.
- `CosmosConfig <configs/cosmos-conf.html>`_: This page lists available Airflow configurations that affect ``astronomer-cosmos`` behavior. You can set them in the ``airflow.cfg`` file or using environment variables.
- `ProfileConfig <profiles/index.html>`_: The ``ProfileConfig`` class determines which data warehouse Cosmos connects to when it executes the dbt SQL. These docs include reference documentation for connecting to popular data warehouses you might use in your dbt code.