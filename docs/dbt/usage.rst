Usage
======

Cosmos supports two standard way of rendering dbt projects: either as a full DAG or as a Task Group.

By default, Cosmos will look in the ``/usr/local/airflow/dbt`` directory (next to the ``dags`` folder if you're using the `Astro CLI <https://github.com/astronomer/astro-cli>`_). You can override this using the ``dbt_root_path`` argument in either :class:`cosmos.providers.dbt.DbtDag` or :class:`cosmos.providers.dbt.DbtTaskGroup`. You can also override the default models directory, which is ``"models"`` by default, using the ``dbt_models_dir`` argument.

Rendering
+++++++++

Full DAG
--------

The :class:`cosmos.providers.dbt.DbtDag` class can be used to render a full DAG for a dbt project. This is useful if you want to run all of the dbt models in a project as a single DAG.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        dbt_args={"schema": "public"},
        dag_id="attribution-playbook",
        start_date=datetime(2022, 11, 27),
        schedule_interval="@daily",
    )


Task Group
----------

The :class:`cosmos.providers.dbt.DbtTaskGroup` class can be used to render a task group for a dbt project. This is useful if you want to run your dbt models in a project as a single task group, and include other non-dbt tasks in your DAG (e.g., extracting and loading data).

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
        )

        e2 = EmptyOperator(task_id="some_extraction")

        e1 >> dbt_tg >> e2


Connections & profiles
+++++++++++++++++++++++++++++++

Cosmos' dbt integration uses Airflow connections to connect to your data sources. You can use the Airflow UI to create connections for your data sources. For more information, see the `Airflow documentation <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection/index.html>`_.

This means you don't need to manage a separate profiles.yml file for your dbt project. Instead, you manage all your connections in one place and can leverage Airflow's connections model (e.g. to use a secrets backend).

To use your Airflow connection, you typically need to pass two parameters to your DAG or Task Group:

1. ``conn_id``: The Airflow connection ID for your data source.
2. ``dbt_args``: A dictionary of dbt arguments to pass to your dbt project. This is where you can specify the schema you want to use for your dbt models.

Under the hood, this information gets translated to a profiles.yml file (using environment variables, not written to the disk) that dbt uses to connect to your data source.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        # ...
        conn_id="airflow_db",
        dbt_args={"schema": "public"},
        # ...
    )
