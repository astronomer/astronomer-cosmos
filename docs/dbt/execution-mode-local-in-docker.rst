Local Execution Mode in Docker
------------------------------

One of the existing `Execution Modes <execution-modes>`_ is ``Docker``, when Cosmos runs each ``dbt`` command in an independent Docker container.

An alternative to the ``Docker`` execution mode is to run Airflow inside ``Docker``,
use the `Local Execution Mode <execution-modes.html#local>`_ and
manage the ``dbt`` installation in an independent Python virtual environment, within the container.

An advantage of this approach when compared to the `Virtualenv Execution Mode <execution-modes.html#virtualenv>`_ is
that there is that there is not an overhead on creating a Python virtualenv each time a Cosmos ``dbt`` task is executed.

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
    from cosmos import DbtTaskGroup

    tg = DbtTaskGroup(
        # ...
        dbt_args={
            # ...
            "dbt_executable_path": f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        }
        # ...
    )
