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

   Callbacks
       User-defined functions that Cosmos invokes after the dbt command completes, during
       post-execution handling and before the temporary project directory is cleaned up.
       Useful for custom logging, alerting, or side effects without modifying the operator.
       Configure them on Cosmos operators via ``callback`` and ``callback_args``; when using
       ``DbtDag`` or ``DbtTaskGroup``, these can be passed through ``operator_args``.
       See :ref:`callbacks`.

   ExecutionConfig
       The Cosmos class used to configure the execution mode and related runtime options such as the
       dbt executable path and invocation mode. See :ref:`execution-config`.

   ExecutionMode
       Describes where and how Cosmos runs dbt commands (e.g. ``LOCAL``, ``WATCHER``, ``DOCKER``).
       Configured via ``ExecutionConfig``. See :ref:`execution-modes`.

   Interceptors
       An optional list of callables (new in Cosmos 1.14) that run before Cosmos builds the dbt
       command for each task. Each callable receives ``(context, operator)`` and may modify
       ``operator.vars`` and ``operator.env``; the modified values are then used when building and
       running the dbt command. Useful for injecting runtime variables or environment values per task
       run. Configured via ``operator_args={"interceptors": [...]}`` on ``DbtDag`` or ``DbtTaskGroup``.
       See :ref:`operator-args`.

   InvocationMode
       Controls how Cosmos calls dbt: ``DBT_RUNNER`` imports dbt as a Python library in the same
       process (no subprocess overhead, lower CPU and memory usage), while ``SUBPROCESS`` spawns a
       separate process (better isolation when dbt and Airflow share a Python environment).
       Running as a subprocess roughly doubles memory usage compared to ``DBT_RUNNER``.
       See :ref:`invocation-mode`.

   LoadMode
       The method Cosmos uses to parse your dbt project (e.g. ``DBT_MANIFEST``, ``DBT_LS``).
       Configured via ``RenderConfig``. See :ref:`parsing-methods`.

   Manifest
       A dbt artifact that contains a full representation of the dbt project's resources, including all
       node configurations and resource properties.
       See `Manifest JSON file <https://docs.getdbt.com/reference/artifacts/manifest-json>`_ in the dbt docs.

   Partial parsing
       A dbt feature, enabled in Cosmos since v1.4, that skips re-parsing files that have not
       changed. Each Airflow node performs a full dbt project parse only once; subsequent task runs
       reuse the cached ``partial_parse.msgpack`` artifact, reducing parse time and CPU usage.
       See :ref:`partial-parsing`.

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

   node_converters
       A ``RenderConfig`` option that accepts a dictionary mapping a ``DbtResourceType`` to a
       callable, allowing users to replace how specific node types are rendered as Airflow tasks.
       See :ref:`render-config`.

   RenderConfig
       The Cosmos class that controls how a dbt project is turned into an Airflow DAG or TaskGroup,
       including node selection, test behaviour, and source rendering. See :ref:`render-config`.

   SourceRenderingBehavior
       Controls whether dbt source nodes are rendered as Airflow tasks. ``NONE`` (default) skips
       source nodes entirely; ``ALL`` renders every source; ``WITH_TESTS_OR_FRESHNESS`` renders only
       sources that have tests or a freshness check defined. Configured via ``RenderConfig``.

   TestBehavior
       Controls when and how Cosmos runs dbt tests. ``AFTER_EACH`` (default) adds a test task after
       each model; ``AFTER_ALL`` runs all tests in a single task at the end; ``BUILD`` runs tests as
       part of ``dbt build`` inside the model task; ``NONE`` skips tests entirely.
       Configured via ``RenderConfig``. See :ref:`testing-behavior`.
