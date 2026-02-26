.. _astro-cli-quickstart:

.. title:: Run Cosmos with the Astro CLI

Run Cosmos with the Astro CLI
=============================

Work locally with Airflow, dbt, and Astronomer Cosmos using the `Astro CLI <https://github.com/astronomer/astro-cli>`_. While Cosmos fully works with standard Airflow and independently of the Astro CLI, the Astro CLI can simplify creating and running Airflow projects. If you want to get started with Cosmos using only Airflow, see `Getting Started on Open Source Airflow <open-source.html>`_.

This guide shows you how to run a simple Dag locally with Cosmos, using an example dbt project in Airflow.

By the end of this quickstart you will:

- Download the Astro CLI
- Set up the Astronomer Cosmos demo project, `cosmos-demo <https://github.com/astronomer/cosmos-demo>`_
- Run a simple Dag that uses dbt to load, run, and test sample e-commerce data

Prerequisites
~~~~~~~~~~~~~~

- Permissions to install software on your machine
- `Python <https://www.python.org/downloads/>`_
- Install `git <https://git-scm.com/install/>`_
- Install a database viewer. This guide uses `dBeaver <https://dbeaver.io/download/>`_
- Install the `Astro CLI <https://docs.astronomer.io/astro/cli/install-cli>`_
- A container manager such as Podman, Docker, or Orbstack. Cosmos requires a container manager, like Docker or Podman, Airflow, and the Astro CLI to run. When you install the Astro CLI, it automatically installs Airflow and Podman, so you can immediately start working with Dags after installing the CLI. If you need to use Docker instead of Podman, see `Switch between Docker and Podman <https://www.astronomer.io/docs/astro/cli/switch-container-management>`_.

Depending on your operating system, you might also need to install a separate installation manager like `Homebrew <https://brew.sh>`_ or `WinGet <https://learn.microsoft.com/en-us/windows/package-manager/winget/>`_.

Clone the demo repo
~~~~~~~~~~~~~~~~~~~

1. Open a terminal in the directory where you want to clone your sample repo.
2. Clone the ``cosmos-demo`` repo.

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-demo.git

Start Airflow locally
~~~~~~~~~~~~~~~~~~~~~

1. Open a terminal at the root of the ``cosmos-demo`` repo.
2. Run ``astro dev start`` to start your Aiflow instance.
3. Open the Airflow UI at ``http://localhost:8080/``.
4. Log in using ``admin`` as both the user name and password to access the **Home** view. This view provides at-a-glance of your overall Airflow environment, including summary statistics about your Dags' performance.

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

For example in the ``stg_customers``, task, in the ``run`` sub-task, the logs include output like:

.. code-block:: text

    ...Running command: ['/usr/local/airflow/dbt_venv/bin/dbt', 'run', '--select', 'stg_customers', '--project-dir', '/tmp/tmp8675309', '--profiles-dir', ...]

This log indicates that the Dag triggers Cosmos to initiate the ``dbt run`` command following the sql actions defined in the ``stg_customers.sql``.

View results with a database viewer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To view the transformations completed by the Dag, you must use a database viewer to access the views and materializations completed by your dbt project.

1. Open dBeaver.
2. Click **Database** on the main menu and then **New database connection**.
3. Select **PostgreSQL** from the list of database types.
4. Add your database connection information. The ``cosmos-demo`` project uses the postgreSQL container that also includes the Airflow metadata database to execute the dbt code. For this project, the postgres connection information is defined in the ``profiles.yml`` file:

.. code-block:: yaml

    airflow_db:
        target: dev
        outputs:
            dev:
            type: postgres
            host: postgres
            user: postgres
            password: postgres
            port: 5432
            dbname: postgres
            schema: dbt
            threads: 4


You have several options to locate this kind of information in dbt projects. For example, the ``cosmos-demo`` project shares this port, username, and password information in the ``profiles.yaml`` file, and the Astro CLI prints it in your terminal after you run ``astro dev start``. When you create your own project, you can use the ``profiles.yml`` file to configure how your dbt project connects to your database.

5. Click **Finish**. dBeaver asks for permission to download the necessary drivers to access and display the database information.
6. After the connection is successful, dBeaver displays the postgres project directory. Navigate to **Tables** to view the different table views created by dbt.

.. codeblock:: text

    ├── postgres localhost:5432
        └── Databases
            └── postgres
                └── schemas
                    └── dbt
                        └── tables
                            ├── customers
                            ├── orders
                            ├── raw_customers
                            ├── raw_orders
                            └── raw_payments

7. **Customers** and **Orders** are the final Table views produced by the dbt code. But click any of these tables and then choose the **Data** tab to see the dbt output.

.. image:: /_static/astro-cli-quickstart-dbeaver.png
   :alt: dBeaver user interface displaying the Customers table view produced by the dbt code. This table includes data that has been joined together from three spearate raw database sources.

Key Concepts
~~~~~~~~~~~~

Congratulations! You ran a dbt project successfully on Airflow! This quickstart includes the minimal steps required to get started working with Cosmos. Specifically it includes:

A ``dockerfile`` that defines
- The Astro Runtime to use for the Airflow project
- The virtual environment where you want to run dbt code
- The connection to the postgres Airflow metadata database

This demo repo also includes a dbt project with configurations that allow you to explore how Cosmos enables Airflow and dbt to work together. These files include:

- ``constants.py``: Points to the dbt project root directory and to the virtual environment configured by the dockerfile that the dbt project uses to execute commands.
- ``profiles.py``: Contains the profile mappings that allow your dbt project to connect to the Airflow metadata database defined in the Airflow ``dockerfile``, where Cosmos runs dbt models for this project.

Cosmos does not require you to use the specific project architecture shown in the ``cosmos-demo`` to run successfully. However, it can serve as a template or example for you to adapt your dbt or Airflow projects to work cohesively.

Next steps
~~~~~~~~~~

- Follow one of the Getting Started Guides where you can bring your own dbt projects and/or Dag code:
    - `Getting Started on Open-Source <open-source.html>`__
    - `Getting Started on Astro <astro.html>`__
    - `Getting Started on MWAA <mwaa.html>`__
    - `Getting Started on GCC <gcc.html>`__

.. - `Getting Started on Azure <azure.html>`_