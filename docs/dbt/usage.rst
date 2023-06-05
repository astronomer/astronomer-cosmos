Usage
======

Cosmos supports two standard way of rendering dbt projects: either as a full DAG or as a Task Group.

By default, Cosmos will look in the ``/usr/local/airflow/dags/dbt`` directory (next to the ``dags`` folder if you're using the `Astro CLI <https://github.com/astronomer/astro-cli>`_). You can override this using the ``dbt_root_path`` argument in either :class:`cosmos.providers.dbt.DbtDag` or :class:`cosmos.providers.dbt.DbtTaskGroup`. You can also override the default models directory, which is ``"models"`` by default, using the ``dbt_models_dir`` argument.

Rendering
---------

Full DAG
++++++++

The :class:`cosmos.providers.dbt.DbtDag` class can be used to render a full DAG for a dbt project. This is useful if you want to run all of the dbt models in a project as a single DAG.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        profile_args={"schema": "public"},
        dag_id="attribution-playbook",
        start_date=datetime(2022, 11, 27),
        schedule_interval="@daily",
    )


Task Group
++++++++++

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
            profile_args={
                "schema": "public",
            },
        )

        e2 = EmptyOperator(task_id="some_extraction")

        e1 >> dbt_tg >> e2


Connections & Profiles
----------------------

See the `Connections & Profiles <connections-and-profiles>`__ page for more information on how to configure your connections and profiles.
