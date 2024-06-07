.. _scheduling:

Scheduling
================

Because Cosmos uses Airflow to power scheduling, you can leverage Airflow's scheduling capabilities to schedule your dbt projects. This includes cron-based scheduling, timetables, and data-aware scheduling. For more info on Airflow's scheduling capabilities, check out the Airflow documentation or check out the `Astronomer documentation <https://docs.astronomer.io/learn/scheduling-in-airflow>`_.

Time-Based Scheduling
----------------------

To schedule a dbt project on a time-based schedule, you can use Airflow's scheduling options. For example, to run a dbt project every day starting on January 1, 2023, you can use the following DAG:

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
    )


Data-Aware Scheduling
---------------------

Apache Airflow 2.4 introduced the concept of `scheduling based on Datasets <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html>`_.

By default, if Airflow 2.4 or higher is used, Cosmos emits `Airflow Datasets <https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html>`_ when running dbt projects. This allows you to use Airflow's data-aware scheduling capabilities to schedule your dbt projects. Cosmos emits datasets using the OpenLineage URI format, as detailed in the `OpenLineage Naming Convention <https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md>`_.

Cosmos calculates these URIs during the task execution, by using the library `OpenLineage Integration Common <https://pypi.org/project/openlineage-integration-common/>`_.

This block illustrates a Cosmos-generated dataset for Postgres:

.. code-block:: python

    Dataset("postgres://host:5432/database.schema.table")


For example, let's say you have:

- A dbt project (``project_one``) with a model called ``my_model`` that runs daily
- A second dbt project (``project_two``) with a model called ``my_other_model`` that you want to run immediately after ``my_model``

We are assuming that the Database used is Postgres, the host is ``host``, the database is ``database`` and the schema is ``schema``.

Then, you can use Airflow's data-aware scheduling capabilities to schedule ``my_other_model`` to run after ``my_model``. For example, you can use the following DAGs:

.. code-block:: python

    from cosmos import DbtDag

    project_one = DbtDag(
        # ...
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
    )

    project_two = DbtDag(
        schedule=[Dataset("postgres://host:5432/database.schema.my_model")],
        dbt_project_name="project_two",
    )

In this scenario, ``project_one`` runs once a day and ``project_two`` runs immediately after ``project_one``. You can view these dependencies in Airflow's UI.
