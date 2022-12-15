.. image:: https://github.com/astronomer/astronomer-cosmos/raw/main/static/banner.png
  :align: center

.. |fury| image:: https://badge.fury.io/py/astronomer-cosmos.svg
    :target: https://badge.fury.io/py/astronomer-cosmos


Astronomer Cosmos |fury|
========================

A framework for dynamically generating `Apache Airflow <https://airflow.apache.org/>`_ DAGs from other tools and frameworks. Develop your workflow in your tool of choice and render it in Airflow as a DAG or Task Group!

Current support for:
 - dbt

Coming soon:
 - Jupyter
 - Hex
 - And more...open an issue if you have a request!

Principles
_____________

`Astronomer Cosmos` is a package to parse and render third-party workflows as Airflow DAGs, `Airflow TaskGroups <https://docs.astronomer.io/learn/task-groups>`_, or individual tasks.

.. image:: https://github.com/astronomer/astronomer-cosmos/raw/main/static/dbt_dag.png
   :width: 800

Cosmos contains `providers` for third-party tools, and each `provider` can be deconstructed into the following components:

- ``parsers``: These are mostly hidden from the end user and are responsible for extracting the workflow from the provider and converting it into ``Task`` and ``Group`` objects. These are executed whenever the Airflow Scheduler heartbeats, allowing us to dynamically render the dependency graph of the workflow.
- ``operators``: These represent the "user interface" of Cosmos -- lightweight classes the user can import and implement in their DAG to define their target behavior. They are responsible for executing the tasks in the workflow.

Cosmos operates on a few guiding principles:

- **Dynamic**: Cosmos generates DAGs dynamically, meaning that the dependency graph of the workflow is generated at runtime. This allows users to update their workflows without having to restart Airflow.
- **Flexible**: Cosmos is not opinionated in that it does not enforce a specific rendering method for third-party systems; users can decide whether they'd like to render their workflow as a DAG, TaskGroup, or individual task.
- **Extensible**: Cosmos is designed to be extensible. Users can add their own parsers and operators to support their own workflows.
- **Modular**: Cosmos is designed to be modular. Users can install only the dependencies they need for their workflows.


Quickstart
_____________

Clone this repository to set up a local environment. Then, head over to our :code:`astronomer-cosmos/examples` directory and follow its README!

Installation
_____________

Install and update using `pip <https://pip.pypa.io/en/stable/getting-started/>`_:

General Installation
********************

.. code-block:: bash

    pip install astronomer-cosmos

Note that this only installs dependencies for the core provider. Read below for more info on how to install specific providers.

Database Specific Installation (dbt)
************************************


To only install the dependencies for a specific databases, specify it in the extra argument as dbt.<database>. For
example, for postgres run:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt.postgres]'

Extras
^^^^^^

.. EXTRA_DOC_START

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Installation Command
     - Dependencies

   * - ``core``
     - ``pip install astronomer-cosmos``
     - apache-airflow, pydantic, Jinja2

   * - ``dbt.all``
     - ``pip install 'astronomer-cosmos[dbt.all]'``
     - astronomer-cosmos, dbt-core, dbt-bigquery, dbt-redshift, dbt-snowflake, dbt-postgres

   * - ``dbt.postgres``
     - ``pip install 'astronomer-cosmos[dbt.postgres]'``
     - astronomer-cosmos, dbt-core, dbt-postgres

   * - ``dbt.bigquery``
     - ``pip install 'astronomer-cosmos[dbt.bigquery]'``
     - astronomer-cosmos, dbt-core, dbt-bigquery

   * - ``dbt.redshift``
     - ``pip install 'astronomer-cosmos[dbt.redshift]'``
     - astronomer-cosmos, dbt-core, dbt-redshift

   * - ``dbt.snowflake``
     - ``pip install 'astronomer-cosmos[dbt.snowflake]'``
     - astronomer-cosmos, dbt-core, dbt-snowflake

Example Usage
_____________

Imagine we have dbt projects located at ``./dbt/{{DBT_PROJECT_NAME}}``. We can render these projects as a Airflow DAGs using the ``DbtDag`` class:

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

Simiarly, we can render these projects as Airflow TaskGroups using the ``DbtTaskGroup`` class. Here's an example with the jaffle_shop project:

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

A detailed overview an how to contribute can be found in the `Contributing Guide <https://github.com/astronomer/astronomer-cosmos/blob/main/CONTRIBUTING.rst>`_.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.


License
_______

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_
