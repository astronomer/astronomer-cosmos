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


Virtual Environment
-------------------

.. tabs::

   .. tab:: Astronomer

        To install dbt into a virtual environment, you can use the following steps:

        1. Create the virtual environment in your Dockerfile (be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``, etc.)

        .. code-block:: docker

            FROM quay.io/astronomer/astro-runtime:8.0.0

            # install dbt into a virtual environment
            RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
                pip install --no-cache-dir <your-dbt-adapter> && deactivate

        2. Add the following to your base project ``requirements.txt`` (preferably pinned)

        .. code-block:: text

            astronomer-cosmos

        3. Use the ``dbt_executable_path`` argument in the Cosmos operator to point to the virtual environment

        .. code-block:: python

            from cosmos.providers.dbt import DbtTaskGroup

            tg = DbtTaskGroup(
                # ...
                dbt_args = {
                    # ...
                    'dbt_executable_path': f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
                }
                # ...
            )


   .. tab:: Docker Image

        To install dbt into a virtual environment on an Airflow Docker Image, you can use the following steps:

        1. Create the virtual environment in your Dockerfile (be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``, etc.)

        .. code-block:: docker

            FROM apache/airflow:2.4.3-python3.10

            # install dbt into a venv to avoid package dependency conflicts
            ENV PIP_USER=false
            RUN python3 -m venv ${AIRFLOW_HOME}/dbt_venv
            RUN ${AIRFLOW_HOME}/dbt_venv/bin/pip install <your-dbt-adapter>
            ENV PIP_USER=true

        3. Add the following to your base project ``requirements.txt`` (preferably pinned)

        .. code-block:: text

            astronomer-cosmos

        4. Use the ``dbt_executable_path`` argument in the Cosmos operator to point to the virtual environment

        .. code-block:: python

            import os
            from cosmos.providers.dbt import DbtTaskGroup

            tg = DbtTaskGroup(
                # ...
                dbt_args = {
                    # ...
                    'dbt_executable_path': f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
                }
                # ...
            )

   .. tab:: MWAA

        .. note::

            This method uses a `startup script with Amazon MWAA <https://docs.aws.amazon.com/mwaa/latest/userguide/using-startup-script.html>`_

        To install dbt into a virtual environment on MWAA, you can use the following steps:

        1. Initialize a startup script as outlined in MWAA's documentation `here <https://docs.aws.amazon.com/mwaa/latest/userguide/using-startup-script.html>`_

        2. Add the following to your startup script (be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``, etc.)

        .. code-block:: shell

            #!/bin/sh

            export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"
            export PIP_USER=false

            python3 -m venv "${DBT_VENV_PATH}"

            ${DBT_VENV_PATH}/bin/pip install <your-dbt-adapter>

            export PIP_USER=true

        3. Add the following to your base project ``requirements.txt`` **preferably pinned to a version that's compatible with your MWAA environment**. To check compatibility, use the `aws mwaa local runner <https://github.com/aws/aws-mwaa-local-runner>`_

        .. code-block:: text

            astronomer-cosmos

        4. Use the ``dbt_executable_path`` argument in the Cosmos operator to point to the virtual environment

        .. code-block:: python

            import os
            from cosmos.providers.dbt import DbtTaskGroup

            tg = DbtTaskGroup(
                # ...
                dbt_args = {
                    # ...
                    'dbt_executable_path': f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
                }
                # ...
            )




Docker and Kubernetes Execution Methods
---------------------------------------

If you intend to use Cosmos with the Docker or Kubernetes execution methods, you will need to install Cosmos with the right optional dependency.

For Kubernetes, you will need to install the ``kubernetes`` extra:

.. code-block:: bash

    pip install 'astronomer-cosmos[..., kubernetes]'

For Docker, you will need to install the ``docker`` extra:

.. code-block:: bash

    pip install 'astronomer-cosmos[..., docker]'
