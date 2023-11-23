.. _self:

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   Home <self>
   Getting Started <getting_started/index>
   Configuration <configuration/index>
   Profiles <profiles/index>
   Contributing <contributing>

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

|fury| |ossrank| |downloads| |pre-commit|

Run your dbt Core projects as `Apache Airflow <https://airflow.apache.org/>`_ DAGs and Task Groups with a few lines of code. Benefits include:

- Run dbt projects against Airflow connections instead of dbt profiles
- Native support for installing and running dbt in a virtual environment to avoid dependency conflicts with Airflow
- Run tests immediately after a model is done to catch issues early
- Utilize Airflow's data-aware scheduling to run models immediately after upstream ingestion
- Turn each dbt model into a task/task group complete with retries, alerting, etc.


Example Usage
___________________

You can render a Cosmos Airflow DAG using the ``DbtDag`` class. Here's an example with the `jaffle_shop project <https://github.com/dbt-labs/jaffle_shop>`_:

..
   The following renders in Sphinx but not Github:

.. literalinclude:: ./dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]


This will generate an Airflow DAG that looks like this:

.. figure:: /docs/_static/jaffle_shop_dag.png

Getting Started
_______________

Check out the Quickstart guide on our `docs <https://astronomer.github.io/astronomer-cosmos/#quickstart>`_. See more examples at `/dev/dags <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_ and at the `cosmos-demo repo <https://github.com/astronomer/cosmos-demo>`_.


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
