Astronomer Cosmos
=================

A framework for generating `Apache Airflow <https://airflow.apache.org/>`_ DAGs from other workflows.

Installation
_____________

Install and update using `pip <https://pip.pypa.io/en/stable/getting-started/>`_:

.. code-block:: bash

    pip install astronomer-cosmos

This only installs dependencies for core provider. To install all dependencies, run:

.. code-block:: bash

    pip install 'astronomer-cosmos[all]'

To only install the dependencies for a specific integration, specify the integration name as extra argument, example
to install dbt integration dependencies, run:

.. code-block:: bash

    pip install 'astronomer-cosmos[dbt]'

Extras
^^^^^^

.. EXTRA_DOC_START

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Installation Command
     - Dependencies

   * - ``all``
     - ``pip install 'astronomer-cosmos[all]'``
     - All

   * - ``dbt``
     - ``pip install 'astronomer-cosmos[dbt]'``
     - dbt core

Example Usage
_____________

Imagine we have a dbt project located at ``./dbt/my_project``.

.. code-block:: python

    from cosmos.providers.dbt import DbtDag, DbtTaskGroup, DbtTask

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


Principles
_____________

`Astronomer Cosmos` provides a framework for generating Apache Airflow DAGs from other workflows. Every provider comes with two main components:

- ``extractors``: These are responsible for extracting the workflow from the provider and converting it into ``Task`` and ``Group`` objects.
- ``operators``: These are used when the workflow is converted into a DAG. They are responsible for executing the tasks in the workflow.

``Astronomer Cosmos`` is not opinionated in the sense that it does not enforce any rendering method. Rather, it comes with the tools to render workflows as Airflow DAGs, task groups, or individual tasks.

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

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_
