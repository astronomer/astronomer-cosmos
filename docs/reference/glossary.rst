Glossary
========

.. glossary::

   Cosmos
       An open-source Python package that allows you to write data transformations using dbt
       and then use Apache Airflow®'s orchestration to integrate dbt projects into end-to-end workflows.

   DAG
       An Airflow term derived from the mathematical structure called **Directed Acyclic Graph**. A DAG
       provides a model that includes everything needed to execute an Airflow workflow.
       See `DAG <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`_.

   DbtDag
       A Cosmos class that wraps a full Airflow DAG around a dbt project, rendering each dbt node as an
       Airflow task. Use ``DbtDag`` when the entire Airflow DAG is dedicated to running a dbt project.
       See :ref:`core-concepts`.

   DbtTaskGroup
       A Cosmos class that embeds a dbt project as an Airflow ``TaskGroup`` inside a larger DAG. Use
       ``DbtTaskGroup`` when dbt is one stage of a broader pipeline (e.g. ingestion → dbt → reporting).
       See :ref:`core-concepts`.

   dbt deps
       A `dbt command <https://docs.getdbt.com/reference/commands/deps>`_ that pulls the most recent
       version of dependencies listed in your ``packages.yml`` file.

   Execution mode
       Describes where and how Cosmos runs dbt commands. You configure it using ``ExecutionConfig``.
       See :ref:`execution-modes`.

   ExecutionConfig
       The Cosmos class used to configure the execution mode and related runtime options such as the
       dbt executable path and invocation mode. See :ref:`execution-config`.

   Load mode
       The method that Cosmos uses to parse your dbt project. See :ref:`parsing-methods`.

   Manifest
       A dbt artifact that contains a full representation of the dbt project's resources, including all
       node configurations and resource properties.
       See `Manifest JSON file <https://docs.getdbt.com/reference/artifacts/manifest-json>`_ in the dbt docs.

   Node
       A dbt concept that encapsulates a step within a pipeline, such as a model, test, seed, or source.

   Partial parsing
       A dbt feature that can greatly speed up parsing and execution when using the ``dbt_ls`` load mode
       by only re-parsing files that have changed. See :ref:`partial-parsing`.

   Profile
       The authentication information used by dbt to connect to your data warehouse.
       See `profile <https://docs.getdbt.com/reference/project-configs/profile>`_.

   ProfileConfig
       The Cosmos class that determines which data warehouse Cosmos connects to when executing dbt SQL.
       See :ref:`connect_database`.

   Profile mapping
       A Cosmos-provided resource that translates Airflow connections into dbt profiles.
       See :ref:`use-profile-mapping`.

   profiles.yml
       The file where dbt stores connection information for each data warehouse connection.
       See :ref:`connect_database`, :ref:`use-your-profiles-yml`, or
       `The profiles.yml file <https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles>`_
       in the dbt docs.

   ProjectConfig
       The Cosmos class used to specify where your dbt project is located and any project variables
       that should be used for rendering and execution.

   RenderConfig
       The Cosmos class that controls how a dbt project is turned into an Airflow DAG or TaskGroup,
       including node selection, test behaviour, and source rendering. See :ref:`render-config`.

   Task
       An Airflow term describing a single unit of work within a DAG.
       See `Task <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html>`_.

   Test
       Refers to the `dbt test command <https://docs.getdbt.com/reference/commands/test>`_. You can
       configure how Cosmos runs ``dbt test`` with :ref:`testing-behavior`.

   Workflow
       A dbt term describing a pipeline that contains a group of steps. dbt can run a subset of tasks
       assuming upstream tasks were already run. This is similar to the Airflow concept of a
       `DAG <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`_.
