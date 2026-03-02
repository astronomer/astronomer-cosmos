.. _oss-quickstart:

Run Cosmos in Open-source Airflow
=================================

Quickly get started working locally with Airflow, dbt, and Astronomer Cosmos using the `Open-source Airflow <https://airflow.apache.org/docs/apache-airflow/stable/index.html>`_.

This quickstart guide shows you how to set up a simple demo project, run a simple Dag locally with Cosmos, and then view the results with an open-source database viewer. If you want to get started working with your own project and configurations, see `Get started with Open-source Airflow <open-source.html>`_.

By the end of this quickstart, you will:

- Set up an Airflow project
- Create a Cosmos project, which includes Dags and a dbt project
- Run your Dag that uses dbt to load, run, and test sample data
- (Optional) View your Dag run output

Prerequisites
~~~~~~~~~~~~~

- Python

1. Install Airflow, Cosmos, and dbt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create your demo project directory.

.. code-block:: bash

    mkdir oss-quickstart
    cd oss-quickstart


2. Create and activate a Python virtual environment in your demo directory.

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate


If you exit your virtual environment, remember you can reactivate it by returning to your project directory and then using the ``source venv/bin/activate`` command.

3. Install Cosmos and SQLite into your virtual environment.

The Cosmos project includes Airflow as a dependency, so when you install Cosmos into your virtual environment, it automatically installs Airflow as well.

.. code-block:: python

    pip install astronomer-cosmos dbt-sqlite


2. Create your Cosmos project structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Set up separate subdirectories in your demo directory for Dags and dbt project:

.. code-block:: bash

    mkdir dags
    mkdir -p dbt_project/micro_project


Your project structure should look like this: ::

    oss-quickstart
    ├── dags/
    └── dbt_project/micro_project/

3. Create a minimal dbt project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the demo dbt project, you need to make some essential components for your project. The dbt commands for this demo take two SQL files, a ``base_model`` that creates a database with greetings, and an ``enriched_model`` that transforms the greetings in the base model.

1. Create your ``dbt_project.yml``

.. code-block:: bash

    touch dbt_project/micro_project/dbt_project.yml


Add the following content to your new project definition.

.. code-block:: yaml

    name: 'micro_project'
    version: '1.0'
    profile: 'micro_project'
    model-paths: ["models"]


2. Create your dbt project's ``profiles.yml``. Cosmos can use this file to connect Airflow to your dbt database, without you needing to specify an Airflow connection.

.. code-block:: bash

    touch dbt_project/micro_project/profiles.yml


Add the following content to your ``profiles.yml``:

.. code-block:: yaml

    micro_project:
    target: dev
    outputs:
        dev:
        type: sqlite
        threads: 1
        database: "my_database.db"
        schema: main


**(Optional) Specify a database to access with a database viewer**

If you want to access the output results of your Cosmos dag, you can add a complete filepath and database name to your ``profiles.yml`` configuration. You can use this filepath later to access your dbt results.

.. code-block:: yaml

    micro_project:
    target: dev
    outputs:
        dev:
        type: sqlite
        threads: 1
        database: database
        schema: main
        schema_and_paths:
                main: <explicit-path-to-demo-project>/oss-quickstart/my_database.db
        schema_directory: <explicit-path-to-demo-project>/oss-quickstart


3. Create a base dbt model

Create a simple dbt model in the ``micro_project`` models.

.. code-block:: bash

    mkdir -p dbt_project/micro_project/models
    echo "select 1 as id, 'hello' as greeting" > dbt_project/micro_project/models/base_model.sql


4. Create an enriched dbt model

Create your enriched dbt model using the following comands.

.. code-block:: bash

    touch dbt_project/micro_project/models/enriched_model.sql


Open the ``enriched_model.sql`` file and add the following commands:

.. code-block:: sql

    select
        id,
        greeting,
        upper(greeting) as greeting_upper,
        length(greeting) as greeting_length
    from {{ ref('base_model') }}


4. Create an Airflow Dag
~~~~~~~~~~~~~~~~~~~~~~~~

Now, in your ``dags`` directory, create an Airflow Dag with the following commands:

.. code-block:: bash

    touch dags/micro_project_dag.py


Add the following Dag Python code to your new file. This Dag tells Airflow and Cosmos where to find the dbt project and profile configurations, which they use to execute the dbt code and write results to the database. This Dag does not include any scheduling information, so you might need to manually trigger Dag runs from the Airflow UI or CLI when you **Run Airflow** at a later step.

.. code-block:: python

    import pathlib
    import os

    from cosmos import DbtDag, ProjectConfig, ProfileConfig

    DBT_PROJECT_PATH = pathlib.Path(
        os.getenv("AIRFLOW_HOME", pathlib.Path(__file__).parent.parent)
    ) / "dbt_project/micro_project"

    micro_project_dag = DbtDag(
        dag_id="micro_project_dag",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="micro_project",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
        )
    )


5. Export environmental variables to Airflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To successfully run your Dag, Airflow needs you to define the some environment variables.
These identify the project home directory, ``AIRFLOW_HOME``, and disable additional Airflow and Cosmos features that are not required for local execution.

.. code-block:: bash

    export AIRFLOW_HOME=`pwd`
    export AIRFLOW__CORE__LOAD_EXAMPLES=false
    export AIRFLOW__COSMOS__ENABLE_TELEMETRY=false


6. Run Airflow
~~~~~~~~~~~~~~

At this point, you've completed the following project setup steps:

- Installed Cosmos, dbt, and Airflow into your environment.
- Created a lightweight dbt project and defined the ``profiles.yml`` file, which Cosmos can use to connect to the dbt database.
- Created an Airflow Dag that defines the ``profile_config`` and ``project_config``, which tells Cosmos the locations of the dbt project and ``profiles.yml`` file.
- Defined the Airflow project home and configured environment variables that improve local Dag performance.


1. Now you can run an Airflow Dag by using ``airflow standalone``, which initializes the database, creates a user, and starts all components at ``localhost: 8080``.

.. code-block:: bash

    airflow standalone


2. Airflow autogenerates credentials when it launches that you must use to access the local Airflow UI.
Open the ``simple_auth_manager_passwords.json.generated`` file in your ``oss-quickstart`` directory. This file contains the ``{"username": "password"}`` key-value pair for you to use to login to ``localhost:8080``.


Troubleshooting
~~~~~~~~~~~~~~~~~~

If you encounter issues, like errror messages that say **Cosmos Dag not loading**, try resetting the Airflow database and reserializing with the following commands.

.. code-block:: bash

    airflow db reset
    airflow dags reserialize
