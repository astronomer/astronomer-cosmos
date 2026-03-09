.. _astro:

Getting Started with Cosmos on Astro
====================================

While it is possible to use Cosmos on Astro with all :ref:`Execution Modes <execution-modes>`, we recommend using the ``local`` execution mode. It's the simplest to set up and use.

If you'd like to see a fully functional project to run in Astro (CLI or Cloud), check out `cosmos-demo <https://github.com/astronomer/cosmos-demo>`_.

Below you can find a step-by-step guide to run your own dbt project within Astro.

Pre-requisites
~~~~~~~~~~~~~~

To get started, you should have:

- The Astro CLI installed. You can find installation instructions `here <https://docs.astronomer.io/astro/cli/install-cli>`_.
- An Astro CLI project. You can initialize a new project with ``astro dev init``.
- A dbt project. The `jaffle shop example <https://github.com/dbt-labs/jaffle-shop-classic>`_ is a good example.

Create a virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a virtual environment in your ``Dockerfile`` using the sample below. Be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``). It's recommended to use a virtual environment because dbt and Airflow can have conflicting dependencies.

.. code-block:: docker

    FROM quay.io/astronomer/astro-runtime:11.3.0

    # install dbt into a virtual environment
    RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
        pip install --no-cache-dir <your-dbt-adapter> && deactivate

An example of dbt adapter is ``dbt-postgres``.

Install Cosmos
~~~~~~~~~~~~~~

Add Cosmos to your project's ``requirements.txt``.

.. code-block:: text

    astronomer-cosmos


Move your dbt project into the DAGs directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Make a new folder, ``dbt``, inside your local project's ``dags`` folder. Then, copy/paste your dbt project into the directory and create a file called ``my_cosmos_dag.py`` in the root of your DAGs directory. Your project structure should look like this:

.. code-block:: text

    ├── dags/
    │   ├── dbt/
    │   │   └── my_dbt_project/
    │   │       ├── dbt_project.yml
    │   │       ├── models/
    │   │       │   ├── my_model.sql
    │   │       │   └── my_other_model.sql
    │   │       └── macros/
    │   │           ├── my_macro.sql
    │   │           └── my_other_macro.sql
    │   └── my_cosmos_dag.py
    ├── Dockerfile
    ├── requirements.txt
    └── ...

Note: your dbt projects can go anywhere on the Airflow image. By default, Cosmos looks in the ``/usr/local/airflow/dags/dbt`` directory, but you can change this by setting the ``dbt_project_dir`` argument when you create your DAG instance.

For example, if you wanted to put your dbt project in the ``/usr/local/airflow/dags/my_dbt_project`` directory, you would do:

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/my_dbt_project",
        ),
        # ...,
    )

Create a dagfile
~~~~~~~~~~~~~~~~

In your ``my_cosmos_dag.py`` file, import the ``DbtDag`` class from Cosmos and create a new DAG instance. Make sure to use the ``dbt_executable_path`` argument to point to the virtual environment you created in step 1.

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.profiles import PostgresUserPasswordProfileMapping

    import os
    from datetime import datetime

    airflow_home = os.environ["AIRFLOW_HOME"]

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={"schema": "public"},
        ),
    )

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            f"{airflow_home}/dags/my_dbt_project",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        # normal dag parameters
        schedule_interval="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="my_cosmos_dag",
        default_args={"retries": 2},
    )

.. note::
   In some cases, especially in larger dbt projects, you might run into a ``DagBag import timeout`` error.
   This error can be resolved by increasing the value of the Airflow configuration `core.dagbag_import_timeout <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dagbag-import-timeout>`_.

Start your project
~~~~~~~~~~~~~~~~~~

Start your project with ``astro dev start``. You should see your Airflow DAG in the Airflow UI (``localhost:8080`` by default), where you can trigger it.

.. image:: /_static/dbt_dag.png
    :alt: Cosmos dbt DAG
    :align: center
