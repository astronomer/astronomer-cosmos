.. _guides:

Guides
======

Cosmos offers a number of configuration options to customize its behavior. For more info, check out the links on the left or the table of contents below.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Set up dbt with Airflow

   dbt_setup/dbt-fusion

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Translating dbt into Airflow

   translate_dbt_to_airflow/map_dbt_to_dags/index
   translate_dbt_to_airflow/testing-behavior
   translate_dbt_to_airflow/translate_nodes/index

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: How Cosmos runs dbt

   run_dbt/execution-modes
   run_dbt/airflow-worker/index
   run_dbt/container/index
   run_dbt/callbacks/callbacks
   run_dbt/operators/operators
   run_dbt/customization/index

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Multi-project Setups

   multi_project/multi-project

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: dbt Documentation

   dbt_docs/generating-docs
   dbt_docs/hosting-docs

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Cosmos DevEx

   cosmos_devex/lineage
   cosmos_devex/compiled-sql
   cosmos_devex/logging
   cosmos_devex/task-display-name