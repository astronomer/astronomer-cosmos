.. _install-options:

Installation Options
====================

Cosmos can run ``dbt`` commands using four different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using a local ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands from Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands from Docker containers managed by Cosmos (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands from Kubernetes Pods managed by Cosmos (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary based on each user's needs and concerns.
Read :ref:`Execution Modes <execution-modes>` to decide which is the most suitable for you.

Depending on the ``execution mode``, the package ``astronomer-cosmos`` should be installed differently.

Local execution mode
--------------------

There are two ways of using the `Local Execution Mode <execution-modes.html#local>`__:

* Installing ``dbt`` together with Cosmos
* Referencing a pre-installed ``dbt`` package

Find more details on how to install Cosmos for each of these below:

Install ``dbt`` together with Cosmos
....................................

If the Airflow host does not have ``dbt``, it is possible to install it as part of the Cosmos package installation,
alongside Airflow and other dependencies:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt-all]'

Using ``dbt-all`` will install Cosmos, ``dbt``, and all supported database dependencies.
If you only need a subset of the supported database types, you can use the following selectors:

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Dependencies

   * - (default)
     - apache-airflow, Jinja2, virtualenv

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

Use a pre-installed ``dbt`` package
.....................................

If the Airflow worker node already has ``dbt``, install a lightweight version of Cosmos by running:

.. code-block:: bash

    pip install astronomer-cosmos

In this case, users can specify - if necessary - a custom path to ``dbt`` by using the argument ``dbt_executable_path``.

For more examples of how to use this execution mode, check the following:

* `Astro Cloud <execution-mode-local-in-astro.html>`__
* `Docker <execution-mode-local-in-docker.html>`__
* `MWAA <execution-mode-local-in-mwaa.html>`__


Virtualenv execution mode
-------------------------

Cosmos can create a dedicated Python virtual environment for each task run, installing ``dbt`` and
any other user-defined dependencies in an isolated way.

In this scenario, install Cosmos using (preferably pinned):

.. code-block:: bash

    pip install astronomer-cosmos

Learn more about this execution mode at `Execution Modes <execution-modes.html#virtualenv>`__.


Docker execution mode
---------------------

Cosmos can run ``dbt`` tasks by running an isolated Docker container per task.
In this case, install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install 'astronomer-cosmos[docker]'

Learn more about this execution mode at `Execution Modes <execution-modes.html#docker>`__.

Kubernetes execution mode
-------------------------

Last but not least, Cosmos can run ``dbt`` tasks by creating a Kubernetes pod per task.
Install the following package at the same level as other Airflow dependencies (preferably pinned):

.. code-block:: bash

    pip install 'astronomer-cosmos[kubernetes]'

Learn more about this execution mode at `Execution Modes <execution-modes.html#kubernetes>`__.
