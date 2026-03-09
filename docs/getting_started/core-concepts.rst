.. _core-concepts:

Cosmos core concepts
====================

**Cosmos** is an open-Source library that helps you to run dbt code in Apache Airflow. Because it operates at the interface between Airflow and dbt, becoming familiar with the commonly used terminology and foundational concepts can help you get started.


Similar dbt and Airflow concepts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dbt and Airflow each solve different kinds of data engineering problems. However, they do share some useful concepts. See :ref:`dbt-airflow-concepts`.

How Cosmos works
~~~~~~~~~~~~~~~~

Cosmos provides an exceptional amount of control and ability to customize how Cosmos runs your dbt project in Airflow. But, when you first get started, you can think of Cosmos as performing two core functions:

- **Parsing**: Cosmos parses the dbt project to map dbt resources into Airflow tasks, and then create tasks in an Airflow Dag.

- **Executing**: Cosmos includes code that executes dbt commands based on user-defined configurations.


DbtDag and DbtTaskGroup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During the dbt code parsing phase, you can choose how you want your dbt workflow to translate into an Airflow Dag:

- **DbtDag**: Renders one dbt project as a complete Dag.
- **DbtTaskGroup**: Renders one dbt project as an Airflow task group within a regular Dag.

ProjectConfig
~~~~~~~~~~~~~

The ProjectConfig contains information about which dbt project a Cosmos Dag or task group  executes, as well as configurations that apply to both, rendering and execution.

See `ProjectConfig reference <../reference/configs/project-config.html>`_.

ProfileConfig
~~~~~~~~~~~~~

The ProfileConfig class determines which data warehouse Cosmos connects to when executing the dbt SQL.

profiles.yml and profile mapping
++++++++++++++++++++++++++++++++

There are two ways to connect Cosmos projects to the data warehouse used by your dbt project:
1. Use an existing ``profiles.yml`` file, by providing the filepath in the ``profiles_yml_filepath`` parameter. Each dbt project contains a ``profiles.yml`` file, which defines how dbt connects to data warehouses and other environments.
2. Use an Airflow connection in your Dag by defining the ``profile_mapping`` and ``conn_id`` key. Airflow connections can be set in various ways, including in the Airflow UI,
as environment variable, or in a secrets backend. Airflow uses connections across its environment in hooks
and operators.

See `Profiles reference <../profiles/index.html>`_ for more information about the ProfileConfig, ``profiles.yml``, and ``profile_mapping``.

Execution Modes
~~~~~~~~~~~~~~~

You define the execution mode for your project by using the ExecutionConfig class, which defines where and how the dbt commands are run within Cosmos.

There are two types of execution modes:

1. **Executing dbt commands on the Airflow worker or triggerer.** These execution modes offer faster
execution times, since no extra container needs to be spun up, but no or limited environment isolation.

2. **Executing dbt commands in a container outside of your Airflow environment.** The Docker or
Kubernetes containers can be spun up in various cloud or on-premises environments

See `Execution modes <../guides/run_dbt/execution-modes.html>`_ and `ExectuionConfig Reference <../reference/configs/execution-config.html>`_.

Parsing
~~~~~~~

Parsing generally refers to the processes and configurations that Cosmos uses to parse your dbt project. See `Parsing Methods <../guides/translate_dbt_to_airflow/parsing-methods.html>`_

Invocation Mode
~~~~~~~~~~~~~~~

The method that Cosmos uses to parse the dbt object, whether its by using the Python ``subprocess`` module or with a ``dbt_runner``. See `Invocation modes <https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/execution-modes.html#invocation-modes>`_.

Rendering
~~~~~~~~~~

After parsing your dbt project, Cosmos *renders* the project as an Airflow Dag or Task Group. Depending on the specifics of your dbt project, you can choose customizations that speed up overall performance. You can find more information about rendering options in the `Render Config <../guides/translate_dbt_to_airflow/render-config.html>`_.


Testing Strategy
++++++++++++++++

By default, Cosmos adds a dbt test after it completes a model. However, you can change this behavior by configuring the test behavior in the RenderConfig. Learn more in `Testing behavior <../guides/translate_dbt_to_airflow/testing-behavior.html>`_.


Select and exclude
++++++++++++++++++++

You can filter your dbt project to only parse a subset of your dbt project by using the RenderConfig, and define the ``select`` and ``exclude`` parameters. See `Selecting and excluding <../guides/translate_dbt_to_airflow/selecting-excluding.html>`_.
