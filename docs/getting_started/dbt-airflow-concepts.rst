.. _dbt-airflow-concepts:

Similar dbt & Airflow concepts
==============================

While dbt is an open source tool for data transformations and analysis, using SQL, Airflow focuses on being a platform
for the development, scheduling and monitoring of batch-oriented workflows, using Python. Although both tools have many
differences, they also share similar concepts.

This page aims to list some of these concepts and help those
who may be new to Airflow or dbt and are considering to use Cosmos.

.. table::
   :align: left
   :widths: auto

   =================================================================================================== ==================================================================================================== ==================================================================================== ======================================================================================================================================================
   Airflow naming                                                                                      dbt naming                                                                                           Description                                                                          Differences
   =================================================================================================== ==================================================================================================== ==================================================================================== ======================================================================================================================================================
   `DAG <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`__             `Workflow <https://docs.getdbt.com/docs/introduction>`__                                             Pipeline (Direct Acyclic Graph) that contains a group of steps                       Airflow expects upstream tasks to have passed to run downstream tasks. dbt can run a subset of tasks assuming upstream tasks were run.
   `Task <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html>`__           `Node <https://docs.getdbt.com/reference/node-selection/syntax>`__                                   Step within a pipeline (DAG or workflow)                                             In dbt, these are usually transformations that run on a remote database. In Airflow, steps can be anything, running locally in Airflow or remotely.
   `Language <https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html>`__  `Language <https://docs.getdbt.com/docs/introduction#dbt-optimizes-your-workflow>`__                 Programming or declarative language used to define pipelines and steps.              In dbt, users write SQL, YML and Python to define the steps of a pipeline. Airflow expects steps and pipelines are written in Python.
   `Variables <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html>`__  `Variables <https://docs.getdbt.com/docs/build/project-variables>`__                                 Key-value configuration that can be used in steps and avoids hard-coded values
   `Templating <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html>`__           `Macros <https://docs.getdbt.com/docs/build/jinja-macros>`__                                         Jinja templating used to access variables, configuration and reference steps         dbt encourages using jinja templating for control structures (if and for). Native in Airflow/Python, used to define variables, macros and filters.
   `Connection <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>`__        `Profile <https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles>`__            Configuration to connect to databases or other services
   `Providers <https://airflow.apache.org/docs/apache-airflow-providers/>`__                           `Adapter <https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/1-what-are-adapters>`__   Additional Python libraries that support specific databases or services
   =================================================================================================== ==================================================================================================== ==================================================================================== ======================================================================================================================================================
