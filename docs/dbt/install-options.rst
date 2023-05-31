.. _install-options:

Installation Options
====================

Cosmos can run ``dbt`` commands using four different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using an user-managed ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands using Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands using Docker (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands within a Kubernetes Pod (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary based on each user's needs and concerns.
Read :ref:`Execution Modes <execution-modes>` to decide which is the most suitable for you.

Depending on the ``execution mode``, the package ``astronomer-cosmos`` should be installed in different ways.

Local execution mode
--------------------

There are two ways of using the :ref:`Local Execution Mode <dbt/execution-modes:Local>`_:
* Installing ``dbt`` within the same environment as Cosmos
* Using an user-managed ``dbt`` installation

More details on how to install Cosmos in each of these scenarios can be found below:

Install ``dbt`` within the same environment as Cosmos
.....................................................

If the Airflow worker node does not have ``dbt`` installed, it is possible to install it alongside Cosmos, Airflow and
other dependencies:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt-all]'

Using ``dbt-all`` will install Cosmos, ``dbt``, and all supported database dependencies.
If you only need a subset of the supported database types, you can use the following selectors:

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


For example, to install Cosmos with ``dbt`` and the Postgres adapter, run the following command:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt-postgres]'

Using an user-managed ``dbt`` binary
....................................

If the Airflow worker node already has ``dbt``, install a lightweight version of Cosmos by running:

.. code-block:: bash

    pip install astronomer-cosmos

In this case, users can specify the path to ``dbt`` by using the argument ``dbt_executable_path``.

Detailed examples of how to use this execution mode can be found at:
* :ref:`Astro Cloud <dbt/execution-mode-local-in-astro>`_
* :ref:`Docker <dbt/execution-mode-local-in-docker>`_
* :ref:`MWAA <dbt/execution-mode-local-in-mwaa>`_


Virtualenv execution mode
-------------------------

Cosmos can create a dedicated Python virtual environment and install DBT for the users, for each task run.

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Virtualenv>`_.


Docker execution mode
---------------------

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install 'astronomer-cosmos[docker]'

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Docker>`_.

Kubernetes execution mode
-------------------------

Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install 'astronomer-cosmos[kubernetes]'

Learn more about this execution mode at :ref:`Execution Modes <dbt/execution-modes:Kubernetes>`_.
