.. _scheduling:

Scheduling
================

Because Cosmos uses Airflow to power scheduling, you can leverage Airflow's scheduling capabilities to schedule your dbt projects. This includes cron-based scheduling, timetables, and data-aware scheduling. For more info on Airflow's scheduling capabilities, check out the Airflow documentation or check out the `Astronomer documentation <https://docs.astronomer.io/learn/scheduling-in-airflow>`_.

Time-Based Scheduling
----------------------

To schedule a dbt project on a time-based schedule, you can use Airflow's scheduling options. For example, to run a dbt project every day starting on January 1, 2023, you can use the following DAG:

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
    )

.. _data-aware-scheduling:

Data-Aware Scheduling
---------------------

`Apache Airflow® <https://airflow.apache.org/>`_ 2.4 introduced the concept of `scheduling based on Datasets <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html>`_.

By default, if using a version between Airflow 2.4 or higher, Cosmos emits `Airflow Datasets <https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html>`_ when running dbt projects. This allows you to use Airflow's data-aware scheduling capabilities to schedule your dbt projects. Cosmos emits datasets using the OpenLineage URI format, as detailed in the `OpenLineage Naming Convention <https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md>`_.

.. important::

   This feature is only available for ``ExecutionMode.LOCAL``, ``ExecutionMode.VIRTUALENV``, ``ExecutionMode.WATCHER`` and ``ExecutionMode.AIRFLOW_ASYNC``.

Cosmos calculates these URIs during the task execution, by using the library `OpenLineage Integration Common <https://pypi.org/project/openlineage-integration-common/>`_.

This block illustrates a Cosmos-generated dataset for Postgres:

.. code-block:: python

    Dataset("postgres://host:5432/database.schema.table")


For example, let's say you have:

- A dbt project (``project_one``) with a model called ``my_model`` that runs daily
- A second dbt project (``project_two``) with a model called ``my_other_model`` that you want to run immediately after ``my_model``

We are assuming that the Database used is Postgres, the host is ``host``, the database is ``database`` and the schema is ``schema``.

Then, you can use Airflow's data-aware scheduling capabilities to schedule ``my_other_model`` to run after ``my_model``. For example, you can use the following DAGs:

.. code-block:: python

    from cosmos import DbtDag

    project_one = DbtDag(
        # ...
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
    )

    project_two = DbtDag(
        schedule=[Dataset("postgres://host:5432/database.schema.my_model")],
        dbt_project_name="project_two",
    )

In this scenario, ``project_one`` runs once a day and ``project_two`` runs immediately after ``project_one``. You can view these dependencies in Airflow's UI.


Examples
.................

This example DAG:

..
   The following renders in Sphinx but not Github:

.. literalinclude:: ../../dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]


Will trigger the following DAG to be run:

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.datasets import Dataset
    from airflow.operators.empty import EmptyOperator

    with DAG(
        "dataset_triggered_dag",
        description="A DAG that should be triggered via Dataset",
        start_date=datetime(2024, 9, 1),
        schedule=[Dataset(uri="postgres://0.0.0.0:5434/postgres.public.orders")],
    ) as dag:
        t1 = EmptyOperator(
            task_id="task_1",
        )
        t2 = EmptyOperator(
            task_id="task_2",
        )

        t1 >> t2


From Cosmos 1.7 and Airflow 2.10, it is also possible to trigger DAGs be to be run by using ``DatasetAliases``:

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.datasets import DatasetAlias
    from airflow.operators.empty import EmptyOperator

    with DAG(
        "datasetalias_triggered_dag",
        description="A DAG that should be triggered via Dataset alias",
        start_date=datetime(2024, 9, 1),
        schedule=[DatasetAlias(name="basic_cosmos_dag__orders__run")],
    ) as dag:

        t3 = EmptyOperator(
            task_id="task_3",
        )

        t3


Known Limitations
.................

Airflow 3.0 and beyond
______________________

Airflow Asset (Dataset) URIs validation rules changed in Airflow 3.0.0 and OpenLineage URIs (standard used by Cosmos) are no longer valid in Airflow 3.

Therefore, if using Cosmos with Airflow 3, the Airflow Dataset URIs will be changed to use slashes instead of dots to separate the schema and table name.

Example of Airflow 2 Cosmos Dataset URI:
- postgres://0.0.0.0:5434/postgres.public.orders

Example of Airflow 3 Cosmos Asset URI:
- postgres://0.0.0.0:5434/postgres/public/orders


If you want to use the Airflow 3 URI standard while still using Airflow 2, please set:

.. code-block:: bash

    export AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1

Remember to update any DAGs that are scheduled using this dataset.

Airflow 2.9 and below
_____________________

If using cosmos with an Airflow 2.9 or below, users will experience the following issues:

- The task inlets and outlets generated by Cosmos will not be seen in the Airflow UI
- The scheduler logs will contain many messages saying "Orphaning unreferenced dataset"

Example of scheduler logs:

.. code-block::

    scheduler | [2023-09-08T10:18:34.252+0100] {scheduler_job_runner.py:1742} INFO - Orphaning unreferenced dataset 'postgres://0.0.0.0:5432/postgres.public.stg_customers'
    scheduler | [2023-09-08T10:18:34.252+0100] {scheduler_job_runner.py:1742} INFO - Orphaning unreferenced dataset 'postgres://0.0.0.0:5432/postgres.public.stg_payments'
    scheduler | [2023-09-08T10:18:34.252+0100] {scheduler_job_runner.py:1742} INFO - Orphaning unreferenced dataset 'postgres://0.0.0.0:5432/postgres.public.stg_orders'
    scheduler | [2023-09-08T10:18:34.252+0100] {scheduler_job_runner.py:1742} INFO - Orphaning unreferenced dataset 'postgres://0.0.0.0:5432/postgres.public.customers'


References about the root cause of these issues:

- https://github.com/astronomer/astronomer-cosmos/issues/522
- https://github.com/apache/airflow/issues/34206


Airflow 2.10.0 and 2.10.1
_________________________

If using Cosmos with Airflow 2.10.0 or 2.10.1, the two issues previously described are resolved, since Cosmos uses ``DatasetAlias``
to support the dynamic creation of datasets during task execution. However, users may face ``sqlalchemy.orm.exc.FlushError``
errors if they attempt to run Cosmos-powered DAGs using ``airflow dags test`` with these versions.

We've reported this issue and it will be resolved in future versions of Airflow:

- https://github.com/apache/airflow/issues/42495

For users to overcome this limitation in local tests, until the Airflow community solves this, we introduced the configuration
``AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS``, that is ``True`` by default. If users want to run ``dags test`` and not see ``sqlalchemy.orm.exc.FlushError``,
they can set this configuration to ``False``. It can also be set in the ``airflow.cfg`` file:

.. code-block::

    [cosmos]
    enable_dataset_alias = False

Starting in Airflow 3, Cosmos users are no longer allowed to set ``AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS`` to ``True``.


Emitting Dataset URIs as XCom
.............................

By default, Cosmos emits datasets as Airflow inlets/outlets but does not expose the raw dataset URIs as XCom values.
If you need access to the dataset URIs (for example, to use them in downstream tasks or for debugging purposes),
you can enable the ``enable_uri_xcom`` setting.

When enabled, Cosmos will push the outlet URIs to XCom with the key ``uri`` after each task execution that emits datasets.

To enable this feature, set the environment variable:

.. code-block:: bash

    export AIRFLOW__COSMOS__ENABLE_URI_XCOM=True

Or in your ``airflow.cfg``:

.. code-block::

    [cosmos]
    enable_uri_xcom = True

When enabled, you can access the URIs in downstream tasks using XCom:

.. code-block:: python

    from airflow.decorators import task


    @task
    def process_uris(**context):
        ti = context["ti"]
        uris = ti.xcom_pull(task_ids="my_dbt_task", key="uri")
        for uri in uris:
            print(f"Processing dataset: {uri}")

.. note::

    This feature is available for all Airflow versions (2.4+) and works alongside the existing dataset emission behavior.
    The ``uri`` XCom contains a list of URI strings, even if there is only one outlet.
