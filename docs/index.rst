.. _self:

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   Home <self>
   Getting Started <getting_started/index>
   Configuration <configuration/index>
   Profiles <profiles/index>
   Project policies <policy/index>

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

To get started right away, check out the `Quickstart Guides <https://astronomer.github.io/astronomer-cosmos/getting_started/index.html>`_.
You can also explore more examples in `/dev/dags <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_
or in the `cosmos-demo repo <https://github.com/astronomer/cosmos-demo>`_.

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

Join the Community
__________________

Have questions, need help, or interested in contributing? We welcome all contributions and feedback!

- Join the community on Slack! You can find us in the Airflow Slack workspace `#airflow-dbt <https://apache-airflow.slack.com/archives/C059CC42E9W>`_ channel. If you don't have an account, click `here <https://apache-airflow-slack.herokuapp.com/>`_ to sign up.
- Report bugs, request features, or ask questions by creating an issue in the `GitHub repository <https://github.com/astronomer/astronomer-cosmos/issues/new/choose>`_.
- Want to contribute new features, bug fixes or documentation enhancements? Please refer to our `Contributing Guide <https://astronomer.github.io/astronomer-cosmos/policy/contributing>`_.`   `
- Check out this `link <https://astronomer.github.io/astronomer-cosmos/policy/contributors>`_. to learn more about our current contributors.

Note that contributors and maintainers are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.

License
_______

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_
