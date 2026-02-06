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
   Airflow 3 compatibility <airflow3_compatibility/index>
   Compatibility Policy <compatibility-policy>

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

Welcome to Astronomer Cosmos! Whether you're an experienced data practitioner or just getting started, Cosmos makes it
simple to manage and orchestrate your dbt workflows using `Apache Airflow® <https://airflow.apache.org/>`_, saving you
time and effort. By automatically turning dbt workflows into Airflow DAGs, Cosmos allows you to focus on building
high-quality data models without the hassle of managing complex integrations.

To get started right away, please check out our `Quickstart Guides <https://astronomer.github.io/astronomer-cosmos/getting_started/index.html>`_.
You can also explore more examples in `/dev/dags <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_
or in the `cosmos-demo repo <https://github.com/astronomer/cosmos-demo>`_.

To learn more about about Cosmos, please read on.


What Is Astronomer Cosmos?
___________________________

Astronomer Cosmos is an open-source library that bridges Apache Airflow and dbt, allowing you to easily transform your
dbt projects into Airflow DAGs and manage everything seamlessly. With Cosmos, you can write your data transformations
using dbt and then schedule and orchestrate them with Airflow, making the entire process smooth and straightforward.

**Why Cosmos?**  Integrating dbt and Airflow can be complex, but Cosmos simplifies it by seamlessly connecting these
powerful tools—letting you focus on what matters most: delivering impactful data models and results without getting
bogged down by technical challenges.


Why Should You Use Cosmos?
___________________________

Cosmos makes orchestrating dbt workflows:

- **Effortless**: Transform your dbt projects into Airflow DAGs without writing extra code—Cosmos handles the heavy lifting.
- **Reliable**: Rely on Airflow's robust scheduling and monitoring features to ensure your dbt workflows run smoothly and efficiently.
- **Scalable**: Easily scale your workflows to match growing data demands, thanks to Airflow's distributed capabilities.

Whether you're handling intricate data tasks or looking to streamline your processes, Cosmos helps you orchestrate dbt
with Airflow effortlessly, saving you time and letting you focus on what truly matters—creating impactful insights.


Example Usage: Jaffle Shop Project
__________________________________

Let's explore a practical example to see how Cosmos can convert the dbt workflow into an Airflow DAG.

The `jaffle_shop project <https://github.com/dbt-labs/jaffle_shop>`_ is a sample dbt project that simulates an e-commerce store's data.
The project includes a series of dbt models that transform raw data into structured tables, such as sales, customers, and products.

Below, you can see what the original dbt workflow looks like in a lineage graph. This graph helps illustrate the
relationships between different models:

.. image:: /_static/jaffle_shop_dbt_graph.png

Cosmos can take this dbt workflow and convert it into an Airflow DAG, allowing you to leverage Airflow's scheduling and
orchestration capabilities.

To convert this dbt workflow into an Airflow DAG, create a new DAG definition file, import ``DbtDag`` from the Cosmos library,
and fill in a few parameters, such as the dbt project directory path and the profile name:

..
   The following renders in Sphinx but not Github:

.. literalinclude:: ./../dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]


This code snippet will generate an Airflow DAG that looks like this:

.. image:: https://raw.githubusercontent.com/astronomer/astronomer-cosmos/main/docs/_static/jaffle_shop_dag.png

``DbtDag`` is a custom DAG generator that converts dbt projects into Airflow DAGs and accepts Cosmos-specific args like
``fail_fast`` to immediately fail a dag if dbt fails to process a resource, or ``cancel_query_on_kill`` to cancel any running
queries if the task is externally killed or manually set to failed in Airflow. ``DbtDag`` also accepts standard DAG arguments such
as ``max_active_tasks``, ``max_active_runs`` and ``default_args``.

With Cosmos, transitioning from a dbt workflow to a proper Airflow DAG is seamless, giving you the best of both tools
for managing and scaling your data workflows.

Getting Started with Airflow Async Execution Mode
-------------------------------------------------

See our :doc:`Getting Started with Airflow Async Execution Mode <getting_started/async-execution-mode>` for details.


Airflow 3 compatibility
-----------------------

See our :doc:`Airflow 3 Compatibility <airflow3_compatibility/index>` for full details.

Changelog
_________

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Refer to `CHANGELOG.rst <https://github.com/astronomer/astronomer-cosmos/blob/main/CHANGELOG.rst>`_
for the latest changes.


Join the Community
__________________

Have questions, need help, or interested in contributing? We welcome all contributions and feedback!

- Join the community on Slack! You can find us in the Airflow Slack workspace `#airflow-dbt <https://apache-airflow.slack.com/archives/C059CC42E9W>`_ channel. If you don't have an account, click `here <https://apache-airflow-slack.herokuapp.com/>`_ to sign up.

- Report bugs, request features, or ask questions by creating an issue in the `GitHub repository <https://github.com/astronomer/astronomer-cosmos/issues/new/choose>`_.

- Want to contribute new features, bug fixes or documentation enhancements? Please refer to our `Contributing Guide <https://astronomer.github.io/astronomer-cosmos/contributing>`_.

- Check out this `link <https://astronomer.github.io/astronomer-cosmos/contributors>`_. to learn more about our current contributors

Note that contributors and maintainers are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.


License
_______

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_


Privacy Notice
______________

The application and this website collect telemetry to support the project's development. These can be disabled by the end-users.

Read the `Privacy Notice <https://github.com/astronomer/astronomer-cosmos/blob/main/PRIVACY_NOTICE.rst>`_ to learn more about it.


.. Tracking pixel for Scarf
.. raw:: html

    <img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=ac335a8b-a9f3-49e6-9e8e-a7ec614fb794" />


Related Repositories
____________________

The following repositories are part of the Astronomer Cosmos project ecosystem:

- `astronomer-cosmos <https://github.com/astronomer/astronomer-cosmos>`_ - The main Cosmos library (this repository)
- `cosmos-demo <https://github.com/astronomer/cosmos-demo>`_ - Example DAGs and demo project for Cosmos

Note: Cosmos does not have any subprojects. The repositories listed above represent the complete project scope.


Security Policy
---------------

Check the project's `Security Policy <https://github.com/astronomer/astronomer-cosmos/blob/main/SECURITY.rst>`_ to learn
how to report security vulnerabilities in Astronomer Cosmos and how security issues reported to the Astronomer Cosmos
security team are handled.
