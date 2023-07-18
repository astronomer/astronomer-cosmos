.. _open-source:

Getting Started on Open Source Airflow
======================================

When running open-source Airflow, your setup may vary. This guide assumes you have access to edit the underlying image.

Create a virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a virtual environment in your ``Dockerfile`` using the sample below. Be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``). It's recommended to use a virtual environment because dbt and Airflow can have conflicting dependencies.

.. code-block:: docker

    FROM my-image:latest

    # install dbt into a virtual environment
    RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
        pip install --no-cache-dir <your-dbt-adapter> && deactivate


Install Cosmos
~~~~~~~~~~~~~~

Install ``astronomer-cosmos`` however you install Python packages in your environment.


Move your dbt project into the DAGs directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Make a new folder, ``dbt``, inside your local project's ``dags`` folder. Then, copy/paste your dbt project into the directory and create a file called ``my_cosmos_dag.py`` in the root of your DAGs directory.

Note: your dbt projects can go anywhere on the Airflow image. By default, Cosmos looks in the ``/usr/local/airflow/dags/dbt`` directory, but you can change this by setting the ``dbt_project_dir`` argument when you create your DAG instance.

For example, if you wanted to put your dbt project in the ``/usr/local/airflow/dags/my_dbt_project`` directory, you would do:

.. code-block:: python

    from cosmos import DbtDag

    my_cosmos_dag = DbtDag(
        dbt_project_dir="/usr/local/airflow/dags/my_dbt_project",
        ...,
    )

Create a dagfile
~~~~~~~~~~~~~~~~

In your ``my_cosmos_dag.py`` file, import the ``DbtDag`` class from Cosmos and create a new DAG instance. Make sure to use the ``dbt_executable_path`` argument to point to the virtual environment you created in step 1.

.. code-block:: python

    from cosmos import DbtDag

    my_cosmos_dag = DbtDag(
        # dbt/cosmos-specific parameters
        dbt_project_name="<my_dbt_project>",
        conn_id="airflow_db",
        profile_args={
            "schema": "public",
        },
        # normal dag parameters
        schedule_interval="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="my_cosmos_dag",
    )
