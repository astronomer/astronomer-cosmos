.. _docker:

Docker Execution Mode
========================================

The following tutorial illustrates how to run the Cosmos dbt Docker Operators and the required setup for them.

Requirements
++++++++++++

1. Docker with docker daemon (Docker Desktop on MacOS). Follow the `Docker installation guide <https://docs.docker.com/engine/install/>`_.
2. Airflow
3. Astronomer-cosmos package containing the dbt Docker operators
4. Postgres docker container
5. Docker image built with required dbt project and dbt DAG
6. dbt DAG with dbt docker operators in the Airflow DAGs directory to run in Airflow

More information on how to achieve 2-6 is detailed below.

Step-by-step instructions
+++++++++++++++++++++++++

**Install Airflow and Cosmos**

Create a python virtualenv, activate it, upgrade pip to the latest version and install `Apache Airflow® <https://airflow.apache.org/>`_ & astronomer-postgres

.. code-block:: bash

    python -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[dbt-postgres]"

**Setup Postgres database**

You will need a postgres database running to be used as the database for the dbt project. Run the following command to run and expose a postgres database

.. code-block:: bash

    docker run --name some-postgres -e POSTGRES_PASSWORD="<postgres_password>" -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres -p5432:5432 -d postgres

**Build the dbt Docker image**

For the Docker operators to work, you need to create a docker image that will be supplied as image parameter to the dbt docker operators used in the DAG.

Clone the `cosmos-example <https://github.com/astronomer/cosmos-example.git>`_ repo

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-example.git
    cd cosmos-example

Create a docker image containing the dbt project files and dbt profile by using the `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/Dockerfile.postgres_profile_docker_k8s>`_, which will be supplied to the Docker operators.

.. code-block:: bash

    docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .

.. note::

    If running on M1, you may need to set the following envvars for running the docker build command in case it fails

    .. code-block:: bash

        export DOCKER_BUILDKIT=0
        export COMPOSE_DOCKER_CLI_BUILD=0
        export DOCKER_DEFAULT_PLATFORM=linux/amd64

Take a read of the Dockerfile to understand what it does so that you could use it as a reference in your project.

    - The `dbt profile <https://github.com/astronomer/cosmos-example/blob/main/example_postgres_profile.yml>`_ file is added to the image
    - The dags directory containing the `dbt project jaffle_shop <https://github.com/astronomer/cosmos-example/tree/main/dags/dbt/jaffle_shop>`_ is added to the image
    - The dbt_project.yml is replaced with `postgres_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/postgres_profile_dbt_project.yml>`_ which contains the profile key pointing to postgres_profile as profile creation is not handled at the moment for K8s operators like in local mode.

**Setup and Trigger the DAG with Airflow**

Copy the dags directory from cosmos-example repo to your Airflow home

.. code-block:: bash

    cp -r dags $AIRFLOW_HOME/

Run Airflow

.. code-block:: bash

    airflow standalone

.. note::

    You might need to run airflow standalone with ``sudo`` if your Airflow user is not able to access the docker socket URL or pull the images in the Kind cluster.

Log in to Airflow through a web browser ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

Enable and trigger a run of the `jaffle_shop_docker <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_docker.py>`_ DAG. You will be able to see the following successful DAG run.

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_docker_dag_run.png
    :width: 800


Specifying ProfileConfig
+++++++++++++++++++++++++

Starting with Cosmos 1.8.0, you can use the ``profile_config`` argument in your Dbt DAG Docker operators to reference
profiles for your dbt project defined in a profiles.yml file. To do so, provide the file’s path via the
``profiles_yml_path`` parameter in ``profile_config``.

Note that in ``ExecutionMode.DOCKER``, the ``profile_config`` is only compatible with the ``profiles_yml_path``
approach. The ``profile_mapping`` method will not work because the required Airflow connections cannot be accessed
within the Docker container to map them to the dbt profile.
