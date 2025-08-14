.. _async-execution-mode:

.. title:: Getting Started with Deferrable Operator

Getting Started with Deferrable Operator
========================================

This guide walks you through setting up an Astro CLI project and running a Cosmos-based DAG with a deferrable operator, enabling asynchronous task execution in Apache Airflow.

Prerequisites
+++++++++++++

- `Astro CLI <https://www.astronomer.io/docs/astro/cli/install-cli>`_

1. Create Astro-CLI Project
+++++++++++++++++++++++++++

Run the following command in your terminal:

.. code-block:: bash

    astro dev init

This will create an Astro project with the following structure:

.. code-block:: bash

    .
    ├── Dockerfile
    ├── README.md
    ├── airflow_settings.yaml
    ├── dags/
    ├── include/
    ├── packages.txt
    ├── plugins/
    ├── requirements.txt
    └── tests/


2. Update Dockerfile
++++++++++++++++++++

Edit your ``Dockerfile`` to ensure add necessary requirements

.. code-block:: bash

    FROM astrocrpublic.azurecr.io/runtime:3.0-2

    # These environment variables configure Cosmos to upload and download
    # compiled SQL files from the specified GCS bucket.
    # The path is set to 'cosmos_remote_target_demo', and access is handled via the 'gcp_conn' Airflow connection.
    ENV AIRFLOW__COSMOS__REMOTE_TARGET_PATH=gs://cosmos_remote_target_demo
    ENV AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID=gcp_conn

3. Add astronomer-cosmos Dependency
+++++++++++++++++++++++++++++++++++

In your ``requirements.txt``, add:

.. code-block:: bash

    astronomer-cosmos[dbt-bigquery, google]>=1.9


4. Create Airflow DAG
+++++++++++++++++++++

1. Create a new DAG file: ``dags/cosmos_async_dag.py``

- Update the ``dataset`` and ``project``

.. code-block:: python

    import os
    from datetime import datetime
    from pathlib import Path

    from cosmos import (
        DbtDag,
        ExecutionConfig,
        ExecutionMode,
        ProfileConfig,
        ProjectConfig,
    )
    from cosmos.constants import TestBehavior
    from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

    DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent / "dbt"
    DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
    DBT_ADAPTER_VERSION = os.getenv("DBT_ADAPTER_VERSION", "1.9")

    cosmos_async_dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
                conn_id="gcp_conn",
                profile_args={
                    "dataset": "cosmos_async_demo",
                    "project": "astronomer-**",
                },
            ),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.AIRFLOW_ASYNC,
            async_py_requirements=[f"dbt-bigquery=={DBT_ADAPTER_VERSION}"],
        ),
        schedule=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        dag_id="cosmos_async_dag",
        operator_args={
            "location": "US",
            "install_deps": True,
            "full_refresh": True,
        },
    )

2. Folder structure for dbt project

- Add a valid dbt project inside your Airflow project under ``dags/dbt/``.


5. Start the Project
++++++++++++++++++++

Launch the Airflow project locally:

.. code-block:: bash

    astro dev start

This will:

- Spin up the scheduler, webserver, and triggerer (needed for deferrable operators)
- Expose Airflow UI at http://localhost:8080

6. Create Airflow Connection
++++++++++++++++++++++++++++

Create an Airflow connection with following configurations

- Connection ID: gcp_conn
- Connection Type: google_cloud_platform
- Extra Fields JSON:

.. code-block:: bash

    {
      "project": "astronomer-**",
      "keyfile_dict": {
        "type": "***",
        "project_id": "***",
        "private_key_id": "***",
        "private_key": "***",
        "client_email": "***",
        "client_id": "***",
        "auth_uri": "***",
        "token_uri": "***",
        "auth_provider_x509_cert_url": "***",
        "client_x509_cert_url": "***",
        "universe_domain": "***"
      }
    }


7. Execute the DAG
++++++++++++++++++

1. Visit the Airflow UI at ``http://localhost:8080``
2. Enable the DAG: ``cosmos_async_dag``
3. Trigger the DAG manually

.. image:: /_static/jaffle_shop_async_execution_mode.png
    :alt: Cosmos dbt Async DAG
    :align: center

The ``run`` tasks will run asynchronously via the deferrable operator, freeing up worker slots while waiting on I/O or long-running tasks.

.. note::
    The deferrable operator is supported for dbt models when using BigQuery.
