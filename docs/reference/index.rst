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

   configs/project-config
   configs/execution-config
   configs/cosmos-conf
   configs/profile-config

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Profiles

   profiles/index

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Additional resources

   glossary


Configurations
~~~~~~~~~~~~~~

There are different configurations and profiles that you can use to configure how Cosmos works.

- :ref:`ProjectConfig <project-config>`: The ``ProjectConfig`` contains information about which dbt project a Cosmos Dag or task group is going to execute, as well as configurations that apply to both rendering and execution.
- :ref:`ExecutionConfig <execution-config>`: The ``ExecutionConfig`` determines where and how the dbt commands are run within Cosmos.
- :ref:`CosmosConfig <cosmos-config>`: This page lists available `Apache Airflow® <https://airflow.apache.org/>`_ configurations that affect ``astronomer-cosmos`` behavior. You can set them in the ``airflow.cfg`` file or using environment variables.
- :ref:`ProfileConfig <profile-config>`: The ``ProfileConfig`` class determines which data warehouse Cosmos connects to when it executes the dbt SQL. These docs include reference documentation for connecting to popular data warehouses you might use in your dbt code.

Profiles
~~~~~~~~

The **Profiles** reference provides information about the different kinds of profile mappings available in Cosmos. These profile mappings are Airflow operators that map Airflow connections to dbt profiles, allowing you to work with resources in your data warehouses.

- :ref:`Profile mapping <profile-mapping-reference>`
