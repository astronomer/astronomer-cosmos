.. _contributing:

Cosmos Contributing Guide
=========================

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.

Learn more about the contributors roles in :ref:`contributors-roles`.

Overview
________

To contribute to the cosmos project:

#. Please create a `GitHub Issue <https://github.com/astronomer/astronomer-cosmos/issues>`_ describing your contribution
#. Open a feature branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch
#. Link your issue to the pull request
#. Once developments are complete on your feature branch, request a review and it will be merged once approved.

Setup local development on host machine
---------------------------------------

This guide will setup astronomer development on host machine, first clone the ``astronomer-cosmos`` repo and enter the repo directory:

.. code-block:: bash

    git clone https://github.com/astronomer/astronomer-cosmos.git
    cd astronomer-cosmos/

Then install ``airflow`` and ``astronomer-cosmos`` using python-venv:

.. code-block:: bash

    python3 -m venv env && source env/bin/activate
    pip3 install "apache-airflow[cncf.kubernetes,openlineage]"
    pip3 install -e ".[dbt-postgres,dbt-databricks]"

Set airflow home to the ``dev/`` directory and disabled loading example DAGs:

.. code-block:: bash

    export AIRFLOW_HOME=$(pwd)/dev/
    export AIRFLOW__CORE__LOAD_EXAMPLES=false

Then, run airflow in standalone mode, command below will create a new user (if not exist) and run necessary airflow component (webserver, scheduler and triggerer):

    By default airflow will use sqlite as database, you can overwrite this by set variable ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`` to the sql connection string.

.. code-block:: bash

    airflow standalone

Once the airflow is up, you can access the Airflow UI at ``http://localhost:8080``.

    Note: whenever you want to start the development server, you need to activate the ``virtualenv`` and set the ``environment variables``

Using Docker Compose for local development
--------------------------------------------

It is also possible to just build the development environment using docker compose

To launch a local sandbox with docker compose, first clone the ``astronomer-cosmos`` repo and enter the repo directory:

.. code-block:: bash

    git clone https://github.com/astronomer/astronomer-cosmos.git
    cd astronomer-cosmos/

To prevent permission error on **Linux**, you must create dags, logs, and plugins folders and change owner to the user ``astro`` with the user ID 50000. To do this, run the following command:

.. code-block:: bash

    mkdir -p dev/dags dev/logs dev/plugins
    sudo chown 50000:50000 -R dev/dags dev/logs dev/plugins

Then, run the docker compose command:

.. code-block:: bash

    docker compose -f dev/docker-compose.yaml up -d --build

Once the sandbox is up, you can access the Airflow UI at ``http://localhost:8080``.

Testing application with hatch
------------------------------

We currently use `hatch <https://github.com/pypa/hatch>`_ for building and distributing ``astronomer-cosmos``.

The tool can also be used for local development. The `pyproject.toml <https://github.com/astronomer/astronomer-cosmos/blob/main/pyproject.toml>`_ file currently defines a matrix of supported versions of Python, Airflow and dbt-core for which a user can run the tests against.

For instance, to run the tests using Python 3.10, `Apache Airflow® <https://airflow.apache.org/>`_ 2.5 and `dbt-core <https://github.com/dbt-labs/dbt-core/>`_ 1.9, use the following:

.. code-block:: bash

    hatch run tests.py3.8-2.4-1.9:test-cov:test-cov

It is also possible to run the tests using all the matrix combinations, by using:

.. code-block:: bash

    hatch run tests:test-cov

The integration tests rely on Postgres. It is possible to host Postgres by using Docker, for example:

.. code-block:: bash

    docker run --name postgres -p 5432:5432 -p 5433:5433 -e POSTGRES_PASSWORD=postgres postgres

To run the integration tests for the first time, use:

.. code-block:: bash

    export AIRFLOW_HOME=`pwd`
    export AIRFLOW_CONN_AIRFLOW_DB=postgres://postgres:postgres@0.0.0.0:5432/postgres
    export DATABRICKS_HOST=''
    export DATABRICKS_TOKEN=''
    export DATABRICKS_WAREHOUSE_ID=''
    export DATABRICKS_CLUSTER_ID=''
    export POSTGRES_PORT=5432
    export POSTGRES_SCHEMA=public
    export POSTGRES_DB=postgres
    export POSTGRES_PASSWORD=postgres
    export POSTGRES_USER=postgres
    export POSTGRES_HOST=localhost
    hatch run tests.py3.8-2.4-1.9:test-cov:test-integration-setup
    hatch run tests.py3.8-2.4-1.9:test-cov:test-integration

If testing for the same Airflow and Python version, next runs of the integration tests can be:

.. code-block:: bash

    hatch run tests.py3.8-2.4-1.9:test-integration

Pre-Commit
----------

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

.. code-block:: bash

    pre-commit install

To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files


Writing Docs
____________

You can run the docs locally by running the following:

.. code-block:: bash

    hatch run docs:serve


This will run the docs server in a virtual environment with the right dependencies. Note that it may take longer on the first run as it sets up the virtual environment, but will be quick on subsequent runs.


Building
________

We use ``hatch`` to build the project. To build the project, run:

.. code-block:: bash

    hatch build


Releasing
_________

We use GitHub actions to create and deploy new releases. To create a new release, first create a new version using:

.. code-block:: bash

    hatch version minor


``hatch`` will automatically update the version for you. Then, create a new release on GitHub with the new version. The release will be automatically deployed to PyPI.

.. note::
    You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.
