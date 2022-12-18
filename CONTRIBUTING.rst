Cosmos Contributing Guide
=========================

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.

Overview
________

To contribute to the cosmos project:

#. Please create a `GitHub Issue <https://github.com/astronomer/astronomer-cosmos/issues>`_ describing your contribution
#. Open a feature branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch
#. Link your issue to the pull request
#. Once developments are complete on your feature branch, request a review and it will be merged once approved.

Creating a Sandbox to Test Changes
__________________________________

Pre-requisites
**************
#. `Astro CLI <https://docs.astronomer.io/astro/cli/install-cli>`_
#. `git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_

Step-by-step
************
To create a sandbox where you can do real-time testing for your proposed to changes to Cosmos, please run these commands
in your terminal:

#. ``git clone git@github.com:astronomer/astronomer-cosmos.git``
#. ``cd astronomer-cosmos/examples``
#. ``mkdir ~/cosmos-sandbox``
#. ``cp -r dags ~/cosmos-sandbox``
#. ``cp -r dbt ~/cosmos-sandbox``
#. ``cp airflow_settings.yaml ~/cosmos-sandbox``
#. ``cp dbt-requirements.txt ~/cosmos-sandbox``
#. ``cd ../.. && mv astronomer-cosmos ~/cosmos-sandbox``
#. ``cd ~/cosmos-sandbox``
#. ``astro dev init``
#. ``rm dags/example_dag_basic.py && rm dags/example_dag_advanced.py``
#. Update the Dockerfile so that it's contents match this::

    FROM quay.io/astronomer/astro-runtime:7.0.0
    ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

    #Installs locally
    USER root
    COPY /astronomer-cosmos/ /astronomer-cosmos
    WORKDIR "/usr/local/airflow/astronomer-cosmos"
    RUN pip install -e .
    USER astro

    #Install then venv /usr/local/airflow/dbt_venv/bin/activate
    WORKDIR "/usr/local/airflow"
    COPY dbt-requirements.txt ./
    RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
        pip install --no-cache-dir -r dbt-requirements.txt && deactivate

#. Create a file in ``~/cosmos-sandbox`` and call it ``docker-compose.override.yml`` with the following::

    version: "3.1"
    services:
      scheduler:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./astronomer-cosmos/cosmos:/usr/local/airflow/cosmos:rw

      webserver:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./astronomer-cosmos/cosmos:/usr/local/airflow/cosmos:rw

      triggerer:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./astronomer-cosmos/cosmos:/usr/local/airflow/cosmos:rw

#. ``astro dev start`` (ensure you are running this from ``~/cosmos-sandbox``)

Pre-Commit
__________

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    pip install -r dev-requirements.txt
    pip install pre-commit
    pre-commit install


To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files
