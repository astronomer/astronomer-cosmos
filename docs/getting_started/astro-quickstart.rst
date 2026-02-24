.. _astro-quickstart:

.. title:: Cosmos Quickstart

Cosmos Quickstart
=================

Work locally with Airflow, dbt, and Astronomer Cosmos using the Astro CLI.

This guide shows you how to run a simple Dag locally with Cosmos, using an example dbt project in Airflow.

By the end of this quickstart you will:

- Download the Astro CLI
- Set up the Astronomer Cosmos demo project, `cosmos-demo <https://github.com/astronomer/cosmos-demo>`_
- Run a simple Dag that uses dbt to load, run, and test sample e-commerce data

Prerequisites
~~~~~~~~~~~~~~

- Permissions to install software on your machine
- Clone the `cosmos-demo <https://github.com/astronomer/cosmos-demo>`_ repo to your machine.
- Install the `Astro CLI <https://docs.astronomer.io/astro/cli/install-cli>`_. 

Cosmos requires a container manager, like Docker or Podman, Airflow, and the Astro CLI to run. When you install the Astro CLI, it automatically installs Airflow and Podman, so you can immediately start working with Dags after installing the CLI. If you need to use Docker instead of Podman, see `Switch between Docker and Podman <https://www.astronomer.io/docs/astro/cli/switch-container-management>`_.

Start Airflow locally
~~~~~~~~~~~~~~~~~~~~~

1. Open a terminal at the root of the ``cosmos-demo`` repo.
2. Run ``astro dev start`` to start your Aiflow instance.
3. Open the Airflow UI at ``http://localhost:8080/`` to access the **Home** view. This view provides at-a-glance of your overall Airflow environment, including summary statistics about your Dags' performance.

The `Airflow UI <https://www.astronomer.io/docs/learn/airflow-ui>`_ enables you to start, stop, troubleshoot, or manage your Dags. 

Run a simple Cosmos Dag
~~~~~~~~~~~~~~~~~~~~~~~

1. From the Airflow Dashboard, click **Dags**. This opens a view where you can see all available Dags. Or, you can see if there were problems loading Dags to your Airflow project.
2. Select **Simple Dag** from the list to access the `Dag view <https://www.astronomer.io/docs/learn/airflow-ui#individual-dag>`_ in the Airflow UI. Click **Code** to see the Dag code.

.. code-block:: python

    simple_dag = DbtDag(
        # dbt/cosmos-specific parameters
        project_config=ProjectConfig(jaffle_shop_path),
        profile_config=airflow_db,
        # The execution_config matches the dbt execution virtual environment defined in the Dockerfile
        execution_config=venv_execution_config,
        # normal dag parameters
        schedule="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="simple_dag",
        tags=["simple"],
    )

3. Click **Trigger** to run the Dag.
4. After the Dag finishes executing. You can select one of the Dag tasks to look at the task **Logs**.
5. In the task logs, you can identify the dbt actions that Cosmos initiates during the Dag.

For example in the `stg_customers`, task, in the `run` sub-task, the logs include output like: 

.. code-block:: text

    ...Running command: ['/usr/local/airflow/dbt_venv/bin/dbt', 'run', '--select', 'stg_customers', '--project-dir', '/tmp/tmp8675309', '--profiles-dir', ...]

This log indicates that the Dag triggers Cosmos to initiate the `dbt run` command following the sql actions in the `stg_customers.sql`.
