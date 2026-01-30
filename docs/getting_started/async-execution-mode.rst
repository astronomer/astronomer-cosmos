.. _async-execution-mode:

.. title:: Getting Started with Deferrable Operator

Airflow Async Execution Mode
============================

This execution mode can reduce the runtime by 35% in comparison to Cosmos LOCAL execution mode, but is currently only available for BigQuery. While this mode was introduced in Cosmos 1.9, we strongly encourage users to use Cosmos 1.11, which has significant performance improvements.

It can be particularly useful for long-running transformations, since it leverages Airflow's `deferrable operators <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`__.

In this mode, there is a ``SetupAsyncOperator`` that will pre-generate the SQL files for the dbt project and upload them to Airflow XCom or a remote location. A remote location will only be used if users set ``AIRFLOW__COSMOS__REMOTE_TARGET_PATH`` and ``AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID``. This operator is run before the remaining pipeline.
All the pipeline dbt model transformations will be run using ``DbtRunAirflowAsyncOperator`` which, instead of running the ``dbt run`` command for each model. They will download the SQL files from the Airflow XCom or remote location and execute them directly leveraging the Airflow ``BigQueryInsertJobOperator``.

Users can leverage other existing ``BigQueryInsertJobOperator`` features, such as the UI controls to link to the job in the BigQuery UI.


Advantages of Airflow Async Mode
++++++++++++++++++++++++++++++++

- **Improved Task Throughput:** Async tasks free up Airflow workers by leveraging the Airflow Trigger framework. While long-running SQL transformations are executing in the data warehouse, the worker is released and can handle other tasks, increasing overall task throughput.
- **Better Resource Utilization:** By minimizing idle time on Airflow workers, async tasks allow more efficient use of compute resources. Workers aren't blocked waiting for external systems and can be reused for other work while waiting on async operations.
- **Faster Task Execution:** With Cosmos ``SetupAsyncOperator``, the SQL transformations are precompiled and uploaded to XCom (default behaviour) or a remote location. Instead of invoking a full dbt run during each dbt model task, the SQL files are downloaded from this XCom or remote path and executed directly. This eliminates unnecessary overhead from running the full dbt command, resulting in faster and more efficient task execution.

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


Control of where to upload the SQL files
++++++++++++++++++++++++++++++++++++++++

For optimal performance we encourage to keep Cosmos standard behaviour (introduced in 1.11), which is to upload the SQL files to XCom, instead of a remote object location.

For the benchmark example described in a previous section, there was an overhead of ~500 seconds with remote SQL file upload/download, but only ~2 seconds using XCom, which can outweigh the performance  improvements introduced by using deferrable operators.

However, if you want to upload the SQL files to a remote object location instead of XCom, you can set the following environment variables:

.. code-block:: bash

    AIRFLOW__COSMOS__REMOTE_TARGET_PATH=gs://cosmos_remote_target_demo
    AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID=gcp_conn


Limitations
+++++++++++


1. **Airflow 2.8 or higher required**: This mode relies on Airflow's `Object Storage <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html>`__ feature, introduced in Airflow 2.8, to store and retrieve compiled SQLs.

2. **Limited to dbt models**: Only dbt resource type models are run asynchronously using Airflow deferrable operators. Other resource types are executed synchronously, similar to the local execution mode.

3. **BigQuery support only**: This mode only supports BigQuery as the target database. If a different target is specified, Cosmos will throw an error indicating the target database is unsupported in this mode. Adding support for other adapters is on the roadmap.

4. **ProfileMapping parameter required**: You need to specify the ``ProfileMapping`` parameter in the ``ProfileConfig`` for your DAG. Refer to the example DAG below for details on setting this parameter.

5. **Location parameter required**: You must specify the location of the BigQuery dataset in the ``operator_args`` of the ``DbtDag`` or ``DbtTaskGroup``. The example DAG below provides guidance on this.

6. **async_py_requirements parameter required**: If you're using the default approach of having a setup task, you must specify the necessary dbt adapter Python requirements based on your profile type for the async execution mode in the ``ExecutionConfig`` of your ``DbtDag`` or ``DbtTaskGroup``. The example DAG below provides guidance on this.

7. **Creation of new isolated virtual environment for each task run**: By default, the ``SetupAsyncOperator`` creates and executes within a new isolated virtual environment for each task run, which can cause performance issues. To reuse an existing virtual environment, use the ``virtualenv_dir`` parameter within the ``operator_args`` of the ``DbtDag``. We have observed that for ``dbt-bigquery``, the ``SetupAsyncOperator`` executes approximately 30% faster when reusing an existing virtual environment, particularly for transformations that take around 10 minutes to complete.

8. **Performance degradation when uploading to remote object location**: Even though it is possible to upload the SQL files to a remote object location by setting environment variables, it is slow. We observed that this introduces a significant overhead in the execution time (500s for 129 models).

9. **TeardownAsyncOperator limitation**: When using a remote object location, in addition to the ``SetupAsyncOperator``, a ``TeardownAsyncOperator`` is also added to the DAG. This task will delete the SQL files from the remote location by the end of the DAG Run. This is can lead to a limitation from a retry perspective, as described in the issue `#2066 <https://github.com/astronomer/astronomer-cosmos/issues/2066>`_. This can be avoided by setting the ``enable_teardown_async_task`` configuration to ``False``, as described in the :ref:`enable_teardown_async_task` section.

For a comparison between different Cosmos execution modes, please, check the :ref:`execution-modes-comparison` section.
