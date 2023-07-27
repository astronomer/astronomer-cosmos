.. |fury| image:: https://badge.fury.io/py/astronomer-cosmos.svg
    :target: https://badge.fury.io/py/astronomer-cosmos

.. |ossrank| image:: https://img.shields.io/endpoint?url=https://ossrank.com/shield/2121
    :target: https://ossrank.com/p/2121-astronomer-cosmos

.. |downloads| image:: https://img.shields.io/pypi/dm/astronomer-cosmos.svg
    :target: https://img.shields.io/pypi/dm/astronomer-cosmos

.. |pre-commit| image:: https://results.pre-commit.ci/badge/github/astronomer/astronomer-cosmos/main.svg
   :target: https://results.pre-commit.ci/latest/github/astronomer/astronomer-cosmos/main
   :alt: pre-commit.ci status



.. image:: https://raw.githubusercontent.com/astronomer/astronomer-cosmos/main/docs/_static/cosmos-logo.svg


===========================================================

|fury| |ossrank| |downloads| |pre-commit|

Run your dbt Core projects as `Apache Airflow <https://airflow.apache.org/>`_ DAGs and Task Groups with a few lines of code. Benefits include:

- Run dbt projects against Airflow connections instead of dbt profiles
- Native support for installing and running dbt in a virtual environment to avoid dependency conflicts with Airflow
- Run tests immediately after a model is done to catch issues early
- Utilize Airflow's data-aware scheduling to run models immediately after upstream ingestion
- Turn each dbt model into a task/task group complete with retries, alerting, etc.

Quickstart
__________

Check out the Quickstart guide on our `docs <https://astronomer.github.io/astronomer-cosmos/#quickstart>`_.


Example Usage
___________________

You can render an Airflow Task Group using the ``DbtTaskGroup`` class. Here's an example with the jaffle_shop project:


.. code-block:: python

    from pendulum import datetime

    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
    from cosmos.profiles import PostgresUserPasswordProfileMapping

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={"schema": "public"},
        ),
    )

    with DAG(
        dag_id="extract_dag",
        start_date=datetime(2022, 11, 27),
        schedule="@daily",
    ):
        e1 = EmptyOperator(task_id="pre_dbt")

        dbt_tg = DbtTaskGroup(
            project_config=ProjectConfig("jaffle_shop"),
            profile_config=profile_config,
        )

        e2 = EmptyOperator(task_id="post_dbt")

        e1 >> dbt_tg >> e2

This will generate an Airflow Task Group that looks like this:

.. figure:: /docs/_static/jaffle_shop_task_group.png

Community
_________
- Join us on the Airflow `Slack <https://join.slack.com/t/apache-airflow/shared_invite/zt-1zy8e8h85-es~fn19iMzUmkhPwnyRT6Q>`_ at #airflow-dbt

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
