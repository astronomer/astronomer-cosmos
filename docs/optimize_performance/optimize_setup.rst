.. _optimize_setup:

Optimize your Cosmos project setup
==================================

How you set up your Cosmos project can impact the overall performance of how Airflow runs your dbt workflow.
You can reduce the execution time by adjusting how you install dependencies, make authentication credentials available to cosmos, and choose an execution mode.


Install dbt and Airflow in the same Python environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can see overall performance improvements when you install dbt in the same Python environment as Airflow.
With this strategy, Cosmos uses dbt as a library instead of subprocess, which reduces memory and CPU demands.

By default, Cosmos uses:

- :ref:`ExecutionMode.LOCAL <local-execution>`
- :ref:`InvocationMode.DBT_RUNNER <invocation-mode>`


Pre-install dbt dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `dbt deps command <https://docs.getdbt.com/reference/commands/deps?version=1.12>` installs all package dependencies for your dbt Core implementation in a ``dbt_packages``
folder in your dbt project. By default Cosmos (re-)runs this command whenever the dbt project is parsed and
in every task execution, but that is often not necessary unless package requirements change.

You can improve your overall project performance by setting up your CI/CD to run ``dbt deps``, and disabling running it in Cosmos by using the :ref:`ProjectConfig <project-config>`.

1. Configure your CI/CD process to run ``dbt deps``, or any similar command that installs dbt dependencies into the project, and make any output artefacts available to Cosmos.

2. Disable running ``dbt deps`` in Cosmos by setting ``install_dbt_deps=False`` in the ``ProjectConfig``:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig

   _project_config = ProjectConfig(
      install_dbt_deps=False
   )
   my_dag = DbtDag(
      # ...
      project_config=_project_config,
   )


Use the profiles.yml file instead of profile mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order for Cosmos to access your dbt warehouse, you must provide connection credentials to Cosmos with either a ``profiles.yml`` file or profile mapping.

When you :ref:`use-profile-mapping`, Cosmos uses an Airflow Connection for your data warehouse credentials. However, when you :ref:`use-your-profiles-yml`, you provide credentials through your dbt project, and Cosmos does not have to parse it as an Airflow connection.

See :ref:`connect_database` for more information about both options.


Execution modes and Dag run performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`execution mode <execution-modes>` you select can directly impact the speed that your Dag runs complete. Some execution modes invoke dbt multiple times while executing the Dag, which can add time.

``ExecutionMode.WATCHER`` and ``ExecutionMode.AIRFLOW_ASYNC`` both address this kind of performance slowdown.

See:

- :ref:`watcher-execution-mode`
- :ref:`async-execution-mode`