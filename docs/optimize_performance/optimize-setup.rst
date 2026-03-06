.. _optimize-set-up:

Optimize your Cosmos project set up
===================================

How you set up your Cosmos project can impact your the overall performance of how Airflow runs your dbt project by addressing the following questions:

- How do you install dbt alongside Airflow?
- How do you install dbt dependencies?

dbt core installation
~~~~~~~~~~~~~~~~~~~~~~

The following steps can help you decide how to install dbt core for your Cosmos implementation.


Determine if you can install dbt and Airflow in the same Python environment
-------------------------------------------------------------------------------

If you can, no cadditional onfiguration is needed.

By default, Cosmos uses:

- ``ExecutionMode.LOCAL``
- ``InvocationMode.DBT_RUNNER``

.. code-block:: python

   FROM quay.io/astronomer/astro-runtime:13.4.0

   RUN python -m venv dbt_venv && \
      source dbt_venv/bin/activate && \
      pip install --no-cache-dir<your-dbt-adapter> && \
      deactivate


Decide if you can create and manage a dedicated Python environment alongside Airflow
---------------------------------------------------------------------------------------

To use a dedicated Python virtual environment, you might need to configure two additional steps:

1. If using Astro, create the virtualenv as part of your Docker image build.
2. Define where the dbt binary for Cosmos to use. This still uses the default ``ExecutionMode.LOCAL``:

.. code-block:: python

   FROM quay.io/astronomer/astro-runtime:13.4.0

   RUN python -m venv dbt_venv && \
      source dbt_venv/bin/activate && \
      pip install --no-cache-dir<your-dbt-adapter> && \
      deactivate
   DbtDag(
   ...,
   execution_config=ExecutionConfig(
         dbt_executable_path=Path("/usr/local/airflow/dbt_venv/bin/dbt")
         operator_args={“py_requirements": ["dbt-postgres==1.6.0b1"]}
   ))


Decide if you want to install dbt in Airflow nodes
-----------------------------------------------------

If you want to install dbt in Airflow nodes, Cosmos can create and manage the dbt Python virtualenv for you with ``ExecutionMode.VIRTUALENV``.

.. code-block:: python

   FROM quay.io/astronomer/astro-runtime:13.4.0

   RUN python -m venv dbt_venv && \
      source dbt_venv/bin/activate && \
      pip install --no-cache-dir<your-dbt-adapter> && \
      deactivate
   DbtDag(
   ...,
   execution_config=ExecutionConfig(
         execution_mode=ExecutionMode.VIRTUALENV,
   )
   )


Use Cosmos without dbt in the Airflow nodes
----------------------------------------------

You don’t have to use dbt in the Airflow nodes to benefit from Cosmos. Instead you can leverage ``LoadMode.DBT_MANIFEST`` or ``ExecutionMode.KUBERNETES``.

.. code-block:: python

   FROM quay.io/astronomer/astro-runtime:13.4.0

   RUN python -m venv dbt_venv && \
      source dbt_venv/bin/activate && \
      pip install --no-cache-dir<your-dbt-adapter> && \
      deactivate
   DbtDag(
   ...,
   execution_config=ExecutionConfig(
         execution_mode=ExecutionMode.VIRTUALENV,
   )
   )

.. _optimize-dbt-deps:

Set up dbt deps
~~~~~~~~~~~~~~~

Determine if you can pre-install dbt dependencies in your Airflow deployment.

If you can, this is the most efficient approach. You can tell Cosmos to ignore the dbt ``packages.yml`` file.

.. code-block:: python

   DbtDag(
  ...,
  render_config=RenderConfig(dbt_deps=False),
  execution_config=ExecutionConfig(
      operator_args={"install_deps": False}
  ))

If you can't pre-install dbt dependencies, Cosmos automatically runs dbt deps before running any dbt command, and does not require any additional configuration.

.. _optimize-database-connections:

Set up database connections
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you manage Airflow database credentials, Cosmos has an extensible set of ``ProfileMapping`` classes, that can automatically create the dbt ``profiles.yml`` from Airflow Connections.

.. code-block:: yaml

   profile_config = ProfileConfig(
    profile_name="my_profile_name",
    target_name="my_target_name",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="my_snowflake_conn_id",
        profile_args={
            "database": "my_snowflake_database",
            "schema": "my_snowflake_schema",
         },
      ),
   )
   dag = DbtDag(
      profile_config=profile_config,
   )

If you don't manage Airflow database credentials, Cosmos also allows you to define your own profiles.yml.

.. code-block:: yaml

   profile_config = ProfileConfig(
    profile_name="my_snowflake_profile",
    target_name="dev",
    profiles_yml_filepath="/path/to/profiles.yml",
   )