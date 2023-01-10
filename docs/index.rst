Cosmos
========================

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   Home <self>
   dbt <dbt/index>
   API Reference <reference/index>
   Contributing <contributing>

.. note::

   Cosmos is under active development. Please open an issue if you have any questions or feedback!

Cosmos is a framework for dynamically generating `Apache Airflow <https://airflow.apache.org/>`_ DAGs from other tools and frameworks. Develop your workflow in your tool of choice and render it in Airflow as a DAG or Task Group!

Current support for:
 - dbt

Coming soon:
 - Jupyter
 - Hex
 - And more...open an issue if you have a request!

Quickstart
========================

dbt
____________________

Install the package:

.. code-block:: bash

    pip install astronomer-cosmos[dbt.all]


Create a DAG and import the ``DbtTaskGroup`` operator. The ``DbtTaskGroup`` operator requires a the name of your dbt project, an Airflow connection ID, a schema, and any additional arguments you'd like to pass to dbt.

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


Principles
========================

`Astronomer Cosmos` is a package to parse and render third-party workflows as Airflow DAGs, `Airflow TaskGroups <https://docs.astronomer.io/learn/task-groups>`_, or individual tasks.

Cosmos contains `providers` for third-party tools, and each `provider` can be deconstructed into the following components:

- ``parsers``: These are mostly hidden from the end user and are responsible for extracting the workflow from the provider and converting it into ``Task`` and ``Group`` objects. These are executed whenever the Airflow Scheduler heartbeats, allowing us to dynamically render the dependency graph of the workflow.
- ``operators``: These represent the "user interface" of Cosmos -- lightweight classes the user can import and implement in their DAG to define their target behavior. They are responsible for executing the tasks in the workflow.

Cosmos operates on a few guiding principles:

- **Dynamic**: Cosmos generates DAGs dynamically, meaning that the dependency graph of the workflow is generated at runtime. This allows users to update their workflows without having to restart Airflow.
- **Flexible**: Cosmos is not opinionated in that it does not enforce a specific rendering method for third-party systems; users can decide whether they'd like to render their workflow as a DAG, TaskGroup, or individual task.
- **Extensible**: Cosmos is designed to be extensible. Users can add their own parsers and operators to support their own workflows.
- **Modular**: Cosmos is designed to be modular. Users can install only the dependencies they need for their workflows.

