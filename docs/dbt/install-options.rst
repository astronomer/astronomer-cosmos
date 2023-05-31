.. _install-options:

Installation Options
====================

Cosmos can run DBT commands using four different approaches, called ``execution modes``:

1. **local**: Run DBT commands from the same environment as Airflow (optional: define the path to a dbt binary)
2. **virtualenv**: Run DBT commands from a Python virtual environment created by Cosmos (**recommended**)
3. **docker**: Run DBT commands using Docker (requires a pre-existing Docker image)
4. **kubernetes**: Run DBT commands within a Kubernetes Pod (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary based on each user's needs and concerns.
Read :ref:`Execution Modes <execution-modes>` to decide which is the most suitable for you.

Depending on the ``execution mode``, the package ``astronomer-cosmos`` should be installed in different ways.

Local execution mode
--------------------

In this case, install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos[dbt-all]


Using ``dbt-all`` will install all Cosmos, dbt, and all of the supported database types. If you only need a subset of the supported database types, you can use the following selectors:

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Dependencies

   * - (default)
     - apache-airflow, Jinja2

   * - ``dbt-all``
     - astronomer-cosmos, dbt-core, dbt-bigquery, dbt-redshift, dbt-snowflake, dbt-postgres

   * - ``dbt-postgres``
     - astronomer-cosmos, dbt-core, dbt-postgres

   * - ``dbt-bigquery``
     - astronomer-cosmos, dbt-core, dbt-bigquery

   * - ``dbt-redshift``
     - astronomer-cosmos, dbt-core, dbt-redshift

   * - ``dbt-snowflake``
     - astronomer-cosmos, dbt-core, dbt-snowflake


For example, to install Cosmos with dbt and the Postgres adapter, run the following command:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt-postgres]'


Virtualenv execution mode
-------------------------

This is the most lightweight form of installing Cosmos.

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Virtualenv>`_.


Docker execution mode
---------------------

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos[docker]

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Docker>`_.

Kubernetes execution mode
-------------------------

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos[kubernetes]

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Kubernetes>`_.
