.. _docker:

Docker execution mode
=====================

The ``docker`` approach assumes you previously created Docker image, which contains all the ``dbt`` pipelines and a ``profiles.yml`` that you manage.

Performance and maintenance considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can have better environment isolation with ``docker`` than when using ``local`` or ``virtualenv`` modes, but this mode also requires more maintenance and has some performance tradeoffs, depending on your project configurations.

Using ``docker`` requires that you ensure the Docker container you use has up-to-date files and you might potentially need to manage secrets in multiple places. Another challenge of working with ``docker`` occurs when the Airflow worker is already running in Docker, which can cause problems related to running `Docker in Docker <https://devops.stackexchange.com/questions/676/why-is-docker-in-docker-considered-bad>`__.

Also, the Docker execution mode approach can be significantly slower than ``virtualenv``, since it might require building the ``Docker`` container before executing dbt commands, which is slower than creating a Virtualenv with ``dbt-core``.

Finally, if you run Airflow in a container - such as in an Astro deployment - you may encounter challenges when attempting to use Cosmos ``ExecutionMode.DOCKER``. Unless you have a strong reason to pursue this setup, we generally advise against running a container inside another container, as discussed in the article `Do not use Docker-in-Docker for CI <https://jpetazzo.github.io/2015/09/03/do-not-use-docker-in-docker-for-ci/>`_.


Set up Docker execution mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following procedure illustrates how to run the Cosmos dbt Docker Operators and the required setup for them.

Requirements
++++++++++++

- Docker with Docker Desktop (Docker Desktop on MacOS) or equivalent (such as `Orbstack <https://orbstack.dev/>`__). Follow the `Docker installation guide <https://docs.docker.com/engine/install/>`_.

The following example setup steps include setting up the following:

- Airflow
- Astronomer-cosmos package containing the dbt Docker operators
- Postgres Docker container
- Docker image built with required dbt project and dbt Dag
- dbt Dag with dbt Docker operators in the Airflow Dags directory to run in Airflow

1. Install Airflow and Cosmos
+++++++++++++++++++++++++++++

Create a python virtualenv, activate it, upgrade pip to the latest version, and install `Apache Airflow® <https://airflow.apache.org/>`_ & ``astronomer-postgres``:

.. code-block:: bash

    python -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[dbt-postgres]"

2. Set up Postgres database
++++++++++++++++++++++++++++

You will need a PostgreSQL database running to use as the database for the dbt project. Run the following command to run and expose a postgres database:

.. code-block:: bash

    docker run --name some-postgres -e POSTGRES_PASSWORD="<postgres_password>" -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres -p5432:5432 -d postgres

3. Build the dbt Docker image
++++++++++++++++++++++++++++++

For the Docker operators to work, you need to create a Docker image that will be supplied as image parameter to the dbt Docker operators used in the Dag.

1. Clone the `cosmos-example <https://github.com/astronomer/cosmos-example.git>`_ repo

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-example.git
    cd cosmos-example

2. Create a Docker image containing the dbt project files and dbt profile by using the `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/Dockerfile.postgres_profile_docker_k8s>`_, which will be supplied to the Docker operators.

.. code-block:: bash

    docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .

.. note::

    If running on M1, you may need to set the following environment variables for running the Docker build command, in case it fails.

    .. code-block:: bash

        export DOCKER_BUILDKIT=0
        export COMPOSE_DOCKER_CLI_BUILD=0
        export DOCKER_DEFAULT_PLATFORM=linux/amd64

Read the following example Dockerfiles to understand what it does so that you can use them as a project reference.

- The `dbt profile <https://github.com/astronomer/cosmos-example/blob/main/example_postgres_profile.yml>`_ file is added to the image.
- The ``dags`` directory containing the `dbt project jaffle_shop <https://github.com/astronomer/cosmos-example/tree/main/dags/dbt/jaffle_shop>`_ is added to the image.
- The ``dbt_project.yml`` is replaced with `postgres_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/postgres_profile_dbt_project.yml>`_, which contains the profile key pointing to ``postgres_profile`` because profile creation is not handled for K8s operators, like in local mode.


4. Set up and trigger the Dag with Airflow
++++++++++++++++++++++++++++++++++++++++++

1. Copy the ``dags`` directory from the ``cosmos-example`` repo to your Airflow home

.. code-block:: bash

    cp -r dags $AIRFLOW_HOME/

This directory contains a Docker-specific example Dag.

2. Run Airflow

.. code-block:: bash

    airflow standalone

.. note::

    You might need to run airflow standalone with ``sudo`` if your Airflow user is not able to access the Docker socket URL or pull the images in the ``Kind`` cluster.

3. Log in to Airflow through a web browser, ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

4. Enable and trigger a run of the `jaffle_shop_docker <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_docker.py>`_ Dag. You can see the following successful Dag run example:

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_docker_dag_run.png
    :width: 800

Specifying ProfileConfig
~~~~~~~~~~~~~~~~~~~~~~~~

Starting with Cosmos 1.8.0, you can use the ``profile_config`` argument in your Dbt Dag Docker operators to reference
profiles for your dbt project defined in a profiles.yml file. To do so, provide the file’s path via the
``profiles_yml_path`` parameter in ``profile_config``.

Note that in ``ExecutionMode.DOCKER``, the ``profile_config`` is only compatible with the ``profiles_yml_path``
approach. The ``profile_mapping`` method will not work because the required Airflow connections cannot be accessed
within the Docker container to map them to the dbt profile.

Troubleshooting
~~~~~~~~~~~~~~~

If dbt is unavailable in the Airflow scheduler, the default ``LoadMode.DBT_LS`` will not work. In this scenario, you must use a :ref:`parsing-methods` that does not rely on dbt, such as ``LoadMode.MANIFEST``.
