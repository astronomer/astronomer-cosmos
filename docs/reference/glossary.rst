.. glossary::

Glossary
=========

Cosmos
    An open-source Python package that allows you to write data transformations using dbt
    and then use  Apache Airflow®'s orchestration to integrate dbt projects into end-to-end workflows.

Dag
    An Airflow term derived from the mathematical structure called **Directed Acyclic Graph**. The Dag provides
    a model that includes everything needed to execute an Airflow workflow. See `Dag <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`_.

dbt deps
    A `dbt command <https://docs.getdbt.com/reference/commands/deps?version=1.12>`_ that pulls the most recent version of dependencies contained in your packages.yml file.

Execution mode
    The execution mode describes where and how Cosmos runs dbt commands. You configure it using the ``ExecutionConfig``. See :ref:`execution-modes`.

Load mode
    The method that Cosmos uses to parse your dbt project. See :ref:`parsing-methods`.

Manifest
    A dbt artifact that This file contains a full representation of the dbt project's resources, including all node configurations and resource properties.
    See `Manifest JSON file <https://docs.getdbt.com/reference/artifacts/manifest-json?version=1.12>`_ in the dbt docs.

Node
    A dbt concept that encapsulates a step within a pipeline.

Partial parsing
    Partial parsing is a dbt feature that can greatly speed up dbt parsing and execution when using the ``dbt_ls``
    load mode. See :ref:`partial-parsing`.

Profile
    The authentication information used by dbt to connect to your data warehouse. See `profile <https://docs.getdbt.com/reference/project-configs/profile?version=1.12>`_.

ProfileConfig
    The class that determines which data warehouse Cosmos connects to when executing the dbt SQL. See :ref:`connect_database`.

Profile mapping
    A Cosmos-provided resource that translate Airflow connections into dbt profiles. See :ref:`use-profile-mapping`.

profiles.yml
    The file where dbt stores connection information for each data warehouse connection. See :ref:`connect_database`, :ref:`use-your-profiles-yml`,
    or `The profiles.yml file <https://docs.getdbt.com/docs/local/connect-data-platform/connection-profiles?version=1.12#about-the-profilesyml-file>`_ in the dbt docs.

ProjectConfig
    The Cosmos class that allows you to specify information about where your dbt project is located and project variables that should be used for rendering and execution.

RenderConfig
    The configuration in Cosmos that controls how Cosmos turns your dbt project into an Airflow dag or task group. :ref:`render-config`.

Task
    An Airflow term describing a step within a pipeline. See `Task <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html>`_.

Test
    Refers to the the `dbt test command <https://docs.getdbt.com/reference/commands/test?version=1.12>`_. You can configure how you want
    Cosmos to handle running ``dbt test`` with :ref:`testing-behavior`.

Workflow
    A dbt term describing a pipeline that contains a group of steps. dbt can run a subset
    of tasks assuming upstream tasks were run. This is similar to the Airflow concept of a `Dag <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`_.
