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
    pip install -r dev-requirements.txt
    pip install pre-commit
    pre-commit install

Embed This Project in Astro
**************

1. run ``git clone git@github.com:astronomer/airflow-dbt-blog.git && cd airflow-dbt-blog``
2. run ``git clone git@github.com:astronomer/cosmos.git``
3. add the following ``docker-compose.override.yml``:

.. code-block:: yaml

  version: "3.1"
  services:
    scheduler:
      volumes:
        - ./dbt:/usr/local/airflow/dbt:rw
        - ./cosmos:/usr/local/airflow/cosmos:rw

    webserver:
      volumes:
        - ./dbt:/usr/local/airflow/dbt:rw
        - ./cosmos:/usr/local/airflow/cosmos:rw

    triggerer:
      volumes:
        - ./dbt:/usr/local/airflow/dbt:rw
        - ./cosmos:/usr/local/airflow/cosmos:rw

4. change the ``Dockerfile`` to be this:

.. code-block:: docker

  FROM quay.io/astronomer/astro-runtime:7.0.0
  ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

  #Installs locally
  USER root
  COPY /cosmos/ /cosmos
  WORKDIR "/usr/local/airflow/cosmos"
  RUN pip install -e .

  WORKDIR "/usr/local/airflow"

  USER astro




To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files
