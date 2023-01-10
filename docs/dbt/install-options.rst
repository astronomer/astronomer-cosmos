Installation Options
=====================

The following options are available to install Cosmos with dbt support:

1. Install Cosmos with a dbt selector from PyPI
2. Install dbt into a virtual environment

Depending on your setup, you may prefer one of these options over the other. Some versions of dbt and Airflow have conflicting dependencies, so you may need to install dbt into a virtual environment.


Direct from PyPI
----------------

To install Cosmos with a dbt selector from PyPI, run the following command:

.. code-block:: bash

    pip install astronomer-cosmos[dbt.all]


Using ``dbt.all`` will install all Cosmos, dbt, and all of the supported database types. If you only need a subset of the supported database types, you can use the following selectors:

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Dependencies

   * - (default)
     - apache-airflow, Jinja2

   * - ``dbt.all``
     - astronomer-cosmos, dbt-core, dbt-bigquery, dbt-redshift, dbt-snowflake, dbt-postgres

   * - ``dbt.postgres``
     - astronomer-cosmos, dbt-core, dbt-postgres

   * - ``dbt.bigquery``
     - astronomer-cosmos, dbt-core, dbt-bigquery

   * - ``dbt.redshift``
     - astronomer-cosmos, dbt-core, dbt-redshift

   * - ``dbt.snowflake``
     - astronomer-cosmos, dbt-core, dbt-snowflake


For example, to install Cosmos with dbt and the Postgres adapter, run the following command:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt.postgres]'


Virtual Environment
-------------------

.. note::

    This assumes you are running Airflow using Docker. If you are running Airflow using a different method, you may need to modify the steps.

To install dbt into a virtual environment, you can use the following steps:

1. Create the virtual environment in your Dockerfile

.. code-block:: bash

    # install dbt into a virtual environment
    # replace dbt-postgres with the adapter you need
    RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
        pip install --no-cache-dir dbt-core dbt-postgres && deactivate

2. Use the ``python_venv`` argument in the Cosmos operator to point to the virtual environment

.. code-block:: python

    from cosmos.providers.dbt import DbtTaskGroup

    tg = DbtTaskGroup(
        # ...
        dbt_args = {
            # ...
            'python_venv': '/usr/local/airflow/dbt_venv/bin/activate'
        }
        # ...
    )

Note that you don't need to install Cosmos into the virtual environment - only dbt and the adapter you need.