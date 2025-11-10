.. _async-execution-mode:

.. title:: Getting Started with Deferrable Operator

Airflow Async Execution Mode
============================

This execution mode can reduce the runtime by 35% in comparison to Cosmos LOCAL execution mode, but is currently only supported for BigQuery. While this mode was introduced in Cosmos 1.9, we strongly encourage users to use Cosmos 1.11, which has significant performance improvements.

It can be particularly useful for long-running transformations, since it leverages Airflow's `deferrable operators <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`__.

In this mode, there is a ``SetupAsyncOperator`` that will pre-generate the SQL files for the dbt project and upload them to Airflow XCom or a remote location. This operator is run before the remaining pipeline.
All the pipeline dbt model transformations will be run using ``DbtRunAirflowAsyncOperator`` which, instead of running the ```dbt run``` command for each model. They will download the SQL files from the Airflow XCom or remote location and execute them directly leveraging the Airflow ``BigQueryInsertJobOperator``.

Users can leverage other existing ``BigQueryInsertJobOperator`` features, such as the buttons to visualise the job in the BigQuery UI.


Advantages of Airflow Async Mode
++++++++++++++++++++++++++++++++

- **Improved Task Throughput:** Async tasks free up Airflow workers by leveraging the Airflow Trigger framework. While long-running SQL transformations are executing in the data warehouse, the worker is released and can handle other tasks, increasing overall task throughput.
- **Better Resource Utilization:** By minimizing idle time on Airflow workers, async tasks allow more efficient use of compute resources. Workers aren't blocked waiting for external systems and can be reused for other work while waiting on async operations.
- **Faster Task Execution:** With Cosmos ``SetupAsyncOperator``, the SQL transformations are precompiled and uploaded to xcom (default behaviour) or a remote location. Instead of invoking a full dbt run during each dbt model task, the SQL files are downloaded from this remote path and executed directly. This eliminates unnecessary overhead from running the full dbt command, resulting in faster and more efficient task execution.

We have `observed <https://github.com/astronomer/astronomer-cosmos/pull/1934>`_ the following performance improvements by running a dbt project with 129 models:

+----------------------------------------------+--------------------------+
| How the dbt pipeline was executed            | Execution Time (seconds) |
+==============================================+==========================+
| ``dbt run`` with dbt Core 1.10               | 13                       |
+----------------------------------------------+--------------------------+
| Cosmos 1.11 with ExecutionMode.LOCAL         | 11                       |
+----------------------------------------------+--------------------------+
| Cosmos 1.11 with ExecutionMode.AIRFLOW_ASYNC | 7                        |
+----------------------------------------------+--------------------------+

For optimal performance we encourage to not upload the SQL files to a remote object location. For this same example, there was an overhead of ~500 seconds with remote SQL file upload/download, but only ~2 seconds using xcom.

Getting Started with Airflow Async Mode
+++++++++++++++++++++++++++++++++++++++

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
            "virtualenv_dir": "dbt_venv",
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


Limitations
+++++++++++


1. The deferrable operator is currently supported for dbt models only when using BigQuery. Adding support for other adapters is on the roadmap.

2. By default, the ``SetupAsyncOperator`` creates and executes within a new isolated virtual environment for each task run, which can cause performance issues. To reuse an existing virtual environment, use the ``virtualenv_dir`` parameter within the ``operator_args`` of the ``DbtDag``. We have observed that for ``dbt-bigquery``, the ``SetupAsyncOperator`` executes approximately 30% faster when reusing an existing virtual environment, particularly for transformations that take around 10 minutes to complete.

    Example:

    .. code-block:: python

        DbtDag(..., operator_args={"virtualenv_dir": "dbt_venv"})


3. It is possible to upload the SQL files to a remote object location by setting the following environment variables. We observed, however, that this introduces a significant overhead in the execution time (500s for 129 models).

    .. code-block:: bash

    AIRFLOW__COSMOS__REMOTE_TARGET_PATH=gs://cosmos_remote_target_demo
    AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID=gcp_conn


4. When using the configuration above, in addition to the ``SetupAsyncOperator``, a ``TeardownAsyncOperator`` is also added to the DAG. This task will delete the SQL files from the remote location.

For more limitations, please, check the :ref:`airflow-async-execution-mode` section.
