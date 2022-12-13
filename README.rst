******
cosmos
******

A framework for generating Apache Airflow DAGs from other workflows.

Principles
**************

`cosmos` provides a framework for generating Apache Airflow DAGs from other workflows. Every provider comes with two main components:

- ``extractors``: These are responsible for extracting the workflow from the provider and converting it into ``Task`` and ``Group`` objects.
- ``operators``: These are used when the workflow is converted into a DAG. They are responsible for executing the tasks in the workflow.

``cosmos`` is not opinionated in the sense that it does not enforce any rendering method. Rather, it comes with the tools to render workflows as Airflow DAGs, task groups, or individual tasks.

Example Usage
**************

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

Development
**************

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    pip install -r dev--requirements.txt
    pip install pre-commit
    pre-commit install


To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files
