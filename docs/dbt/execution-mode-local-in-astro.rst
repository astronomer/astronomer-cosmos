Local Execution Mode in Astro
-----------------------------

It is possible to use Cosmos in Astro by using several :ref:`Execution Modes <execution-modes>`, including ``local``, ``virtualenv``, ``docker`` and ``kubernetes``.

Below is an example of how to use the `Local Execution Mode <execution-modes.html#local>`__
and the ``dbt_executable_path`` argument.

The step-by-step is detailed below.

1. Create the virtual environment in your ``Dockerfile`` (be sure to replace ``<your-dbt-adapter>`` with the actual adapter you need (i.e. ``dbt-redshift``, ``dbt-snowflake``, etc.)

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
        dbt_args={
            # ...
            "dbt_executable_path": f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        }
        # ...
    )
