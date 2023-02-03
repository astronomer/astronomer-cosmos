Cosmos - dbt Support
====================

Cosmos allows you to render your dbt models as Airflow DAGs and Task Groups.

.. toctree::
    :maxdepth: 2
    :caption: Sections

    Installation Options <install-options>
    Usage <usage>
    Scheduling <scheduling>
    Configuration <configuration>


Quickstart
----------

Install the package using pip:

.. code-block:: bash

    pip install astronomer-cosmos[dbt.all]


Create a DAG and import the :class:`cosmos.providers.dbt.DbtTaskGroup` class. The ``DbtTaskGroup`` operator requires a the name of your dbt project, an Airflow connection ID, a schema, and any additional arguments you'd like to pass to dbt.

.. code-block:: python

    from pendulum import datetime

    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from cosmos.providers.dbt.task_group import DbtTaskGroup


    with DAG(
        dag_id="extract_dag",
        start_date=datetime(2022, 11, 27),
        schedule="@daily",
    ) as dag:

        e1 = EmptyOperator(task_id="ingestion_workflow")

        dbt_tg = DbtTaskGroup(
            group_id="dbt_tg",
            dbt_project_name="jaffle_shop",
            conn_id="airflow_db",
            dbt_args={
                "schema": "public",
            },
            dag=dag,
        )

        e2 = EmptyOperator(task_id="some_extraction")

        e1 >> dbt_tg >> e2


The ``DbtTaskGroup`` operator will automatically generate a TaskGroup with the tasks defined in your dbt project. Here's what the DAG looks like in the Airflow UI:


.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/dbt_dag.png
   :width: 800
   
   dbt's default jaffle_shop project rendered as a TaskGroup in Airflow