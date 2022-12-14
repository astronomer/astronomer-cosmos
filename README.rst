******
Astronomer Cosmos
******

A framework for generating Apache Airflow DAGs from other workflows.

Principles
**************

`Astronomer Cosmos` provides a framework for generating Apache Airflow DAGs from other workflows. Every provider comes with two main components:

- ``extractors``: These are responsible for extracting the workflow from the provider and converting it into ``Task`` and ``Group`` objects.
- ``operators``: These are used when the workflow is converted into a DAG. They are responsible for executing the tasks in the workflow.

``Astronomer Cosmos`` is not opinionated in the sense that it does not enforce any rendering method. Rather, it comes with the tools to render workflows as Airflow DAGs, task groups, or individual tasks.

Example Usage
_____________


Imagine we have a dbt project located at ``./dbt/my_project``.

.. code-block:: python

    from astronomer.cosmos.providers.dbt import DbtDag, DbtTaskGroup, DbtTask

    # render as a DAG
    dag = DbtDag(
        project_dir="./dbt/my_project",
        dag_id="my_dag",
        schedule_interval="@daily",
        default_args={"owner": "airflow"},
    )

    # render as a task group
    with DAG("my_dag", default_args={"owner": "airflow"}) as dag:
        task_group = DbtTaskGroup(
            project_dir="./dbt/my_project",
            task_group_id="my_task_group",
        )

    # render as an individual task
    with DAG("my_dag", default_args={"owner": "airflow"}) as dag:
        task = DbtTask(
            project_dir="./dbt/my_project",
            task_id="my_task",
        )


Principle
---------

We will only create Async operators for the "sync-version" of operators that do some level of polling
(take more than a few seconds to complete).

For example, we won’t create an async Operator for a ``BigQueryCreateEmptyTableOperator`` but will create one
for ``BigQueryInsertJobOperator`` that actually runs queries and can take hours in the worst case for task completion.

To create async operators, we need to inherit from the corresponding airflow sync operators.
If sync version isn't available, then inherit from airflow ``BaseOperator``.

To create async sensors, we need to inherit from the corresponding sync sensors.
If sync version isn't available, then inherit from airflow ``BaseSensorOperator``.

Changelog
---------

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/cosmos/blob/main/CHANGELOG.rst>`_
for the latest changes.

Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the `Contributing Guide <https://github.com/astronomer/cosmos/blob/main/CONTRIBUTING.rst>`_.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/cosmos/blob/main/CODE_OF_CONDUCT.md>`_.

Goals for the project
_____________________

- Goal 1
- Goal 2
- Goal 3

Limitations
___________

- List any limitations

License
_______

`Apache License 2.0 <https://github.com/astronomer/cosmos/blob/main/LICENSE>`_
