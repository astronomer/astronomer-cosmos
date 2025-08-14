.. _async-execution-mode:

.. title:: Getting Started with Deferrable Operator

Airflow Async Execution Mode
============================

The Airflow async execution mode in Cosmos designed to improve pipeline performance. This execution mode could be preferred when you’ve long running resources and you want to run them asynchronously by leveraging Airflow’s `deferrable operators<https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`_. In this mode, additional operators—``SetupAsyncOperator`` and ``TeardownAsyncOperator``—are added to your workflow.

- **SetupAsyncOperator:** This task runs a mocked ``dbt run`` command on your dbt project, which outputs compiled SQL files to the project’s target directory. These compiled SQLs are then uploaded to a remote location specified by the ``remote_target_path`` configuration.
- **TeardownAsyncOperator:** This task deletes the resources created by ``SetupAsyncOperator`` from the remote location defined by the ``remote_target_path`` configuration.

Advantages of Airflow Async Mode
++++++++++++++++++++++++++++++++

- **Improved Task Throughput:** Async tasks free up Airflow workers by leveraging the Airflow Trigger framework. While long-running SQL transformations are executing in the data warehouse, the worker is released and can handle other tasks, increasing overall task throughput.
- **Faster Task Execution:** With Cosmos ``SetupAsyncOperator``, transformations can be executed without invoking the full dbt run command. This reduces the overhead of running unnecessary dbt commands, improving the performance of each task.
- **Better Resource Utilization:** By minimizing idle time on Airflow workers, async tasks allow more efficient use of compute resources. Workers aren't blocked waiting for external systems and can be reused for other work while waiting on async operations.

Getting Started Airflow Async Mode
++++++++++++++++++++++++++++++++++

This guide walks you through setting up an Astro CLI project and running a Cosmos-based DAG with a deferrable operator, enabling asynchronous task execution in Apache Airflow.

Prerequisites
+++++++++++++

- `Astro CLI <https://www.astronomer.io/docs/astro/cli/install-cli>`_
- Airflow>=2.8

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

Edit your Dockerfile to ensure all necessary requirements are included.

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
    The deferrable operator is currently supported for dbt models only when using BigQuery. Adding support for other adapters is on the roadmap.
