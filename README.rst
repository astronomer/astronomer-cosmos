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

Run your dbt Core projects as `Apache Airflow® <https://airflow.apache.org/>`_ DAGs and Task Groups with a few lines of code. Benefits include:

- Run dbt projects against Airflow connections instead of dbt profiles
- Native support for installing and running dbt in a virtual environment to avoid dependency conflicts with Airflow
- Run tests immediately after a model is done to catch issues early
- Utilize Airflow's data-aware scheduling to run models immediately after upstream ingestion
- Turn each dbt model into a task/task group complete with retries, alerting, etc.

Quickstart
__________

Check out the Getting Started guide on our `docs <https://astronomer.github.io/astronomer-cosmos/getting_started/index.html>`_. See more examples at `/dev/dags <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_ and at the `cosmos-demo repo <https://github.com/astronomer/cosmos-demo>`_.


Example Usage
___________________

You can render a Cosmos Airflow DAG using the ``DbtDag`` class. Here's an example with the `jaffle_shop project <https://github.com/dbt-labs/jaffle_shop>`_:

..
   This renders on Github but not Sphinx:

https://github.com/astronomer/astronomer-cosmos/blob/24aa38e528e299ef51ca6baf32f5a6185887d432/dev/dags/basic_cosmos_dag.py#L1-L42

This will generate an Airflow DAG that looks like this:

.. figure:: https://github.com/astronomer/astronomer-cosmos/blob/main/docs/_static/jaffle_shop_dag.png


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


Privacy Notice
______________

The application and this website collect telemetry to support the project's development. These can be disabled by the end-users.

Read the `Privacy Notice <https://github.com/astronomer/astronomer-cosmos/blob/main/PRIVACY_NOTICE.rst>`_ to learn more about it.

.. Tracking pixel for Scarf

.. image:: https://static.scarf.sh/a.png?x-pxid=ae43a92a-5a21-4c77-af8b-99c2242adf93
   :target: https://static.scarf.sh/a.png?x-pxid=ae43a92a-5a21-4c77-af8b-99c2242adf93


Security Policy
---------------

Check the project's `Security Policy <https://github.com/astronomer/astronomer-cosmos/blob/main/SECURITY.rst>`_ to learn
how to report security vulnerabilities in Astronomer Cosmos and how security issues reported to the Astronomer Cosmos
security team are handled.
