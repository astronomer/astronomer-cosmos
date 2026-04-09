.. _core-concepts:

Cosmos core concepts
--------------------

**Cosmos** is an open-source library that helps you to run dbt code in Apache Airflow. Because it operates at the interface between Airflow and dbt, becoming familiar with the commonly used terminology and foundational concepts can help you get started.


Similar dbt and Airflow concepts
++++++++++++++++++++++++++++++++

dbt and Airflow each solve different kinds of data engineering problems. However, they do share some useful concepts. See :ref:`dbt-airflow-concepts`.

How Cosmos works
++++++++++++++++

Cosmos provides an exceptional amount of control and ability to customize how Cosmos runs your dbt project in Airflow. When starting with Cosmos, you can think of it as performing two core functions:

- **Parsing**: Cosmos parses the dbt project to map dbt resources into Airflow tasks, and then create tasks in an Airflow Dag.

- **Executing**: Cosmos includes code that executes dbt commands based on user-defined configurations.


DbtDag and DbtTaskGroup
+++++++++++++++++++++++

During the dbt code parsing phase, you can choose how you want your dbt workflow to translate into an Airflow Dag:

- **DbtDag**: Renders one dbt project as a complete Dag.
- **DbtTaskGroup**: Renders one dbt project as an Airflow task group within a regular Dag.

You can see example Dags for both a simple `DbtDag <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/basic_cosmos_dag.py>`_ and `DbtTaskGroup <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/basic_cosmos_task_group.py>`_ in the Cosmos `dev/dags directory <https://github.com/astronomer/astronomer-cosmos/tree/main/dev/dags>`_.

ProjectConfig
+++++++++++++

The ``ProjectConfig`` contains information about which dbt project a Cosmos Dag or task group  executes, as well as configurations that apply to both, rendering and execution. See :ref:`project-config`.

ProfileConfig
+++++++++++++

The ``ProfileConfig`` class determines which data warehouse Cosmos connects to when executing the dbt SQL.

Connecting Cosmos to your data warehouse
''''''''''''''''''''''''''''''''''''''''

There are two ways to connect Cosmos projects to the data warehouse used by your dbt project:

1. Use an existing ``profiles.yml`` file, by providing the filepath in the ``profiles_yml_filepath`` parameter. Each dbt project contains a ``profiles.yml`` file, which defines how dbt connects to data warehouses and other environments.
2. Use an Airflow connection by defining the ``profile_mapping`` and ``conn_id`` keys in your Dag. Airflow connections can be set in various ways, including in the Airflow UI, as an environment variable, or in a secrets backend. Airflow uses connections across its environment in hooks and operators.

See :ref:`profile-config` for more information about the ``ProfileConfig``, ``profiles.yml``, and ``profile_mapping``.

Execution Modes
+++++++++++++++

You define the execution mode for your project by using the ``ExecutionConfig`` class, which determines where and how dbt commands are run within Cosmos.

There are two main categories of execution modes:

1. **Executing dbt commands on the Airflow worker.**
   In these execution modes, dbt commands run directly on the Airflow worker. These modes typically offer faster execution because no additional container needs to be created, but they provide limited environment isolation. Examples include:

   - **Local execution**, where dbt runs directly in the worker environment.
   - **Virtualenv execution**, where Cosmos creates an isolated Python virtual environment on the worker to run dbt with its own dependencies.

2. **Executing dbt commands in a container outside of the Airflow worker environment.**
   In these modes, Cosmos runs dbt commands inside Docker or Kubernetes containers. This provides stronger environment isolation and allows you to run dbt in environments with different dependencies or system configurations.

See :ref:`execution-modes` and :ref:`execution-config`.

Parsing
+++++++

Parsing generally refers to the processes and configurations that Cosmos uses to parse your dbt project. See :ref:`parsing-methods`.

Invocation Mode
+++++++++++++++

The method that Cosmos uses to parse the dbt object, whether its by using the Python ``subprocess`` module or with a ``dbt_runner``. See `Invocation modes <https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/execution-modes.html#invocation-modes>`_.

Rendering
+++++++++

After parsing your dbt project, Cosmos *renders* the project as an Airflow Dag or Task Group. Depending on the specifics of your dbt project, you can choose customizations that speed up overall performance. You can find more information about rendering options in the `RenderConfig <../guides/translate_dbt_to_airflow/render-config.html>`_.


Testing Strategy
''''''''''''''''

By default, Cosmos adds a dbt test after it completes a model. However, you can change this behavior by configuring the test behavior in the ``RenderConfig``. Learn more in :ref:`testing-behavior`.


Select and exclude
''''''''''''''''''

You can filter your dbt project to only parse a subset of your dbt project by using the ``RenderConfig``, and define the ``select`` and ``exclude`` parameters. See :ref:`selecting-excluding`.
