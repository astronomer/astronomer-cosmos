.. _mwaa:

Getting Started with Cosmos on Amazon Managed Workflows
=======================================================

Users can face Python dependency issues when trying to use the Cosmos `Local Execution Mode <execution-modes.html#local>`_ in Amazon Managed Workflows for `Apache Airflow® <https://airflow.apache.org/>`_ (MWAA).

This step-by-step illustrates how to use the Local Execution Mode, together with the
`MWAA's startup script <https://docs.aws.amazon.com/mwaa/latest/userguide/using-startup-script.html>`_ and
the ``dbt_executable_path`` argument.

Create a Startup Script
-----------------------

MWAA allows users to run a startup script before the scheduler and webserver are started. This is a great place to install dbt into a virtual environment.

To do so:

1. Initialize a startup script as outlined in MWAA's documentation `here <https://docs.aws.amazon.com/mwaa/latest/userguide/using-startup-script.html>`_

2. Add the following to your startup script (be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``, etc.)

.. code-block:: shell

    #!/bin/sh

    export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"

    python3 -m venv "${DBT_VENV_PATH}"

    ${DBT_VENV_PATH}/bin/pip install <your-dbt-adapter>


Install Cosmos
--------------

Add the following to your base project ``requirements.txt``:

.. code-block:: text

    astronomer-cosmos


Move your dbt project into the DAGs directory
---------------------------------------------

Make a new folder, ``dbt``, inside your local ``dags`` folder. Then, copy/paste your dbt project into the directory and create a file called ``my_cosmos_dag.py`` in the root of your DAGs directory. Your folder structure should look like this:

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
    └── ...

Note: your dbt projects can go anywhere that Airflow can access. By default, Cosmos looks in the ``/usr/local/airflow/dags/dbt`` directory, but you can change this by setting the ``dbt_project_dir`` argument when you create your DAG instance.

For example, if you wanted to put your dbt project in the ``/usr/local/airflow/dags/my_dbt_project`` directory, you would do:

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/my_dbt_project",
        ),
        # ...,
    )


Create your DAG
---------------

In your ``my_cosmos_dag.py`` file, import the ``DbtDag`` class from Cosmos and create a new DAG instance. Make sure to use the ``dbt_executable_path`` argument to point to the virtual environment you created in step 1.

.. code-block:: python

    import os
    from datetime import datetime
    from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.profiles import PostgresUserPasswordProfileMapping

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={"schema": "public"},
        ),
    )

    execution_config = ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    )

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            "<my_dbt_project>",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
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
