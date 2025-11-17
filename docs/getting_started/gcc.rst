.. _gcc:

Getting Started on Google Cloud Composer (GCC)
================================================

Because there's no straightforward way of creating a Python virtual environment in Google Cloud Composer (GCC) , we recommend using Cosmos' built-in virtual environment functionality to run dbt.

Install Cosmos
--------------

Add the following to your base project ``requirements.txt``:

.. code-block:: text

    astronomer-cosmos


Move your dbt project into the DAGs directory
---------------------------------------------

Make a new folder, ``dbt``, inside your local ``dags`` folder. Then, copy/paste your dbt project into the directory and create a file called ``my_cosmos_dag.py`` in the root of your DAGs directory.

Note: your dbt projects can go anywhere that Airflow can read. By default, Cosmos looks in the ``/usr/local/airflow/dags/dbt`` directory, but you can change this by setting the ``dbt_project_dir`` argument when you create your DAG instance.

For more accurate parsing of your dbt project, you should pre-compile your dbt project's ``manifest.json`` (include ``dbt deps && dbt compile`` as part of your deployment process).

For example, if you wanted to put your dbt project in the ``/usr/local/airflow/dags/my_dbt_project`` directory, you would do:

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig

    my_cosmos_dag = DbtDag(
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/my_dbt_project",
            manifest_path="/usr/local/airflow/dags/my_dbt_project/target/manifest.json",
        ),
        # ...,
    )


.. note::
   You can also exclude the ``manifest_path=...`` from the ``ProjectConfig``. Excluding a ``manifest_path`` file will by default use Cosmos's ``custom`` parsing method, which may be less accurate at parsing a dbt project compared to providing a ``manifest.json``.

Create your DAG
---------------

In your ``my_cosmos_dag.py`` file, import the ``DbtDag`` class from Cosmos and create a new DAG instance. You need to supply additional arguments in the ``operator_args`` dictionary to tell Cosmos which packages are required.

Make sure to rename the ``<your-adapter>`` value below to your adapter's Python package (i.e. ``dbt-snowflake`` or ``dbt-bigquery``)

If you need to modify the pip install options, you can do so by adding ``pip_install_options`` to the ``operator_args`` dictionary. For example, if you wanted to install packages from local wheels you could set it too: ``["--no-index", "--find-links=/path/to/wheels"]``. See documentation for available `pip install options <https://pip.pypa.io/en/stable/cli/pip_install/>`_.

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.constants import ExecutionMode
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
            "<my_dbt_project>",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["<your-adapter>"],
        },
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
