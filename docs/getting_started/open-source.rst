.. _open-source:

Getting Started with Cosmos on Open-source Airflow
==================================================

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

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            "/usr/local/airflow/dags/my_dbt_project",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
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
