.. image:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/banner.png
  :align: center

.. |fury| image:: https://badge.fury.io/py/astronomer-cosmos.svg
    :target: https://badge.fury.io/py/astronomer-cosmos
    
.. |ossrank| image:: https://img.shields.io/endpoint?url=https://ossrank.com/shield/2121
    :target: https://ossrank.com/shield/2121


Astronomer Cosmos |fury| |ossrank|
==================================

A framework for dynamically generating `Apache Airflow <https://airflow.apache.org/>`_ DAGs from other tools and frameworks. Develop your workflow in your tool of choice and render it in Airflow as a DAG or Task Group!

Current support for:
 - dbt

Coming soon:
 - Jupyter
 - Hex
 - And more...open an issue if you have a request!

Quickstart
__________

Check out the Quickstart guide on our `docs <https://astronomer.github.io/astronomer-cosmos/#quickstart>`_.


Example Usage (dbt)
___________________

Cosmos lets you render dbt projects as Airflow DAGs and Task Groups. To render a DAG, import ``DbtDag`` and point it to your dbt project.

.. code-block:: python

    from pendulum import datetime
    from airflow import DAG
    from cosmos.providers.dbt.dag import DbtDag

    # dag for the project jaffle_shop
    jaffle_shop = DbtDag(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        dbt_args={
            "schema": "public",
        },
        dag_id="jaffle_shop",
        start_date=datetime(2022, 11, 27),
    )

Simiarly, you can render an Airflow TaskGroups using the ``DbtTaskGroup`` class. Here's an example with the jaffle_shop project:

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

Changelog
_________

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/astronomer-cosmos/blob/main/CHANGELOG.rst>`_
for the latest changes.

Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the `Contributing Guide <https://astronomer.github.io/astronomer-cosmos/contributing>`_.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.


License
_______

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_
