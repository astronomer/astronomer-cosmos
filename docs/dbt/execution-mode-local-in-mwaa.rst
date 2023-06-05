Local Execution Mode in MWAA
----------------------------

Users can face Python dependency issues when trying to use the Cosmos `Local Execution Mode <execution-modes.html#local>`_ in Amazon Managed Workflows for Apache Airflow (MWAA).

This step-by-step illustrates how to use the Local Execution Mode, together with the
`MWAA's startup script <https://docs.aws.amazon.com/mwaa/latest/userguide/using-startup-script.html>`_ and
the ``dbt_executable_path`` argument.

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
        dbt_args={
            # ...
            "dbt_executable_path": f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        }
        # ...
    )
