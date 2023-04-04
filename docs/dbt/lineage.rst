Configuring Lineage
===================

Cosmos uses the `dbt-ol <https://openlineage.io/blog/dbt-with-marquez/>`_ wrapper to emit lineage events to OpenLineage.
Follow the instructions below to ensure Cosmos is configured properly to do this. If you are using a virtual environment
to separate dbt requirements from Airflow as outlined `here </astronomer-cosmos/dbt/install-options.html#virtual-environment>`_, then please follow the ``venv`` tab below. Otherwise,
reference the ``base`` tab instructions

.. tabs::

   .. tab:: base

        **Prerequisites**

        - `Setup OpenLineage and Airflow <https://docs.astronomer.io/learn/airflow-openlineage>`_

        **Enabling Lineage**

        To enable lineage on Add the following to your requirements.txt file:

        .. code-block:: text

            astronomer-cosmos[dbt-openlineage]

   .. tab:: venv

        **Prerequisites**

        - `Setup OpenLineage and Airflow <https://docs.astronomer.io/learn/airflow-openlineage>`_

        **Enabling Lineage**

        Step 1: Add steps in your ``Dockerfile`` for the venv and wrapping the dbt executable

        .. code-block:: Docker

            FROM quay.io/astronomer/astro-runtime:7.2.0

            # install python virtualenv to run dbt
            WORKDIR /usr/local/airflow
            COPY dbt-requirements.txt ./
            RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
                pip install --no-cache-dir -r dbt-requirements.txt && deactivate

            # wrap the executable from the venv so that dbt-ol can access it
            RUN echo -e '#!/bin/bash' > /usr/bin/dbt && \
                echo -e 'source /usr/local/airflow/dbt_venv/bin/activate && dbt "$@"' >> /usr/bin/dbt

            # ensure all users have access to the executable
            RUN chmod -R 777 /usr/bin/dbt

        Step 2: Create a ``dbt-requirements.txt`` file with the following contents. If you're using a different
        data warehouse than Redshift, then replace with the one that you're using (i.e. ``dbt-bigquery``,
        ``dbt-snowflake``, etc.)

        .. code-block:: text

            dbt-redshift
            openlineage-dbt

        Step 3: Add the following to your ``requirements.txt`` file

        .. code-block:: text

            astronomer-cosmos

        Step 4: When instantiating a Cosmos object be sure to use the ``dbt_executable_path`` parameter for the dbt-ol
        installed

        .. code-block:: python

            jaffle_shop = DbtTaskGroup(
                ...,
                dbt_args={
                    "dbt_executable_path": "/usr/local/airflow/dbt_venv/bin/dbt-ol",
                }
            )
