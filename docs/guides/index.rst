.. _guides:

Guides
======

.. toctree::
   :maxdepth: 0
   :hidden:

   self

Cosmos offers a number of configuration options to customize how Airflow dags and dbt commands run.

To set up a project, you follow the same general set of steps.


Set up dbt with Airflow
~~~~~~~~~~~~~~~~~~~~~~~~~~

Make your dbt projects available to Airflow and install dbt into the environment where your dbt code runs.

.. toctree::
   :maxdepth: 1
   :caption: Set up dbt with Airflow

   dbt_setup/dbt-fusion
   dbt_setup/execution-modes-local-conflicts

Connect to your dbt database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure your Cosmos project to allow Airflow Dags to initiate dbt commands, and make data transformations and updates in your data warehouses. You can create these connections with your ``profiles.yml`` file in the dbt project, using profile mappings, or customizing ``ProfileConfig`` per dbt configuration.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Connect to your database

   Connection options <connect_database/index>
   connect_database/use-your-profiles-yml
   connect_database/use-profile-mapping
   connect_database/profile-customise-per-node


Translate your dbt code into Airflow Dags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can customize how Cosmos parses your dbt workflows into Airflow Dags. Choosing how you want your dbt nodes to map to Airflow tasks within Dags can affect the time required for Cosmos to parse the dbt workflows and for Airflow to execute the resulting Dags.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Translating dbt into Airflow

   translate_dbt_to_airflow/parsing-methods
   Selecting what to run <translate_dbt_to_airflow/selecting-excluding>
   Configure tests <translate_dbt_to_airflow/testing-behavior>
   translate_dbt_to_airflow/managing-sources
   translate_dbt_to_airflow/render-config
   Customize node conversion <translate_dbt_to_airflow/dag-customization>


Run dbt
~~~~~~~~~~~~~

Specify more details about how Cosmos runs both dbt commands and Airflow Dags. This includes :ref:`execution-modes` , either one that runs dbt on an Airflow worker node or one that runs in a container. You can customize additional aspects of how your dbt code runs, like using particular operators that correspond to dbt commands. And, you can leverage Airflow's scheduling capabilities in your Cosmos Dags.

.. toctree::
   :maxdepth: 1
   :caption: How Cosmos runs dbt

   run_dbt/execution-modes
   run_dbt/airflow-worker/index
   run_dbt/container/index
   run_dbt/callbacks/callbacks
   run_dbt/operators/operators
   run_dbt/customization/index

Multi-project Setups
~~~~~~~~~~~~~~~~~~~~

If you have a multi-project architecture where you have multiple dbt projects that reference each others' models, you can set up ``dbt-loom`` with Cosmos to handle cross-project references.

.. toctree::
   :maxdepth: 1
   :caption: Multi-project Setups

   Handle cross-project references <multi_project/multi-project>

Add your dbt documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

Cosmos supports dbt's documentation capabilities.

.. toctree::
   :maxdepth: 1
   :caption: dbt Documentation

   dbt_docs/generating-docs
   dbt_docs/hosting-docs


Cosmos DevEx
~~~~~~~~~~~~

You can configure Cosmos to improve your development experience.

.. toctree::
   :maxdepth: 1
   :caption: Cosmos DevEx

   cosmos_devex/lineage
   cosmos_devex/compiled-sql
   cosmos_devex/logging
   cosmos_devex/task-display-name
