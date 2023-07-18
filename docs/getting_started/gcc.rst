.. _gcc:

Getting Started on GCC
=======================

Because there's no straightforward way of creating a Python virtual environment in GCC, we recommend using Cosmos' built-in virtual environment functionality to run dbt.

Install Cosmos
--------------

Add the following to your base project ``requirements.txt``:

.. code-block:: text

    astronomer-cosmos


Move your dbt project into the DAGs directory
---------------------------------------------

Make a new folder, ``dbt``, inside your local ``dags`` folder. Then, copy/paste your dbt project into the directory and create a file called ``my_cosmos_dag.py`` in the root of your DAGs directory.

Note: your dbt projects can go anywhere that Airflow can read. By default, Cosmos looks in the ``/usr/local/airflow/dags/dbt`` directory, but you can change this by setting the ``dbt_project_dir`` argument when you create your DAG instance.

For example, if you wanted to put your dbt project in the ``/usr/local/airflow/dags/my_dbt_project`` directory, you would do:

.. code-block:: python

    from cosmos import DbtDag

    my_cosmos_dag = DbtDag(
        dbt_project_dir="/usr/local/airflow/dags/my_dbt_project",
        ...,
    )


Create your DAG
---------------

In your ``my_cosmos_dag.py`` file, import the ``DbtDag`` class from Cosmos and create a new DAG instance. You need to supply additional arguments in the ``operator_args`` dictionary to tell Cosmos which packages are required.

Make sure to rename the ``<your-adapter>`` value below to your adapter's Python package (i.e. ``dbt-snowflake`` or ``dbt-bigquery``)

.. code-block:: python

    from cosmos import DbtDag

    my_cosmos_dag = DbtDag(
        # dbt/cosmos-specific parameters
        dbt_project_name="<my_dbt_project>",
        conn_id="airflow_db",
        profile_args={
            "schema": "public",
        },

        # cosmos virtualenv parameters
        execution_mode="virtualenv",
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["<your-adapter>"],
        },

        # normal dag parameters
        schedule_interval="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="my_cosmos_dag",
    )
