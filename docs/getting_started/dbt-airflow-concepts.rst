.. _dbt-airflow-concepts:

Similar dbt & Airflow concepts
==============================

While dbt is an open source tool for data transformations and analysis, using SQL, Airflow focuses on being a platform
for the development, scheduling and monitoring of batch-oriented workflows, using Python. Although both tools have many
differences, they also share similar concepts.

This page aims to list some of these concepts and help those
who may be new to Airflow or dbt and are considering to use Cosmos.


+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Airflow naming | dbt naming   | Description                                                                     | Differences                                                                 | References                                                                           |
+================+==============+=================================================================================+=============================================================================+======================================================================================+
| DAG            | Workflow     | Pipeline (Direct Acyclic Graph) that contains a group of steps                  | Airflow expects upstream tasks to have passed to run downstream tasks.      | https://airflow.apache.org/docs/apache-airflow/2.7.1/core-concepts/dags.html         |
|                |              |                                                                                 | dbt can run a subset of tasks assuming upstream tasks were run.             | https://docs.getdbt.com/docs/introduction                                            |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Task           | Node         | Step within a pipeline (DAG or workflow)                                        | In dbt, these are usually transformations that run on a remote database.    | https://docs.getdbt.com/reference/node-selection/syntax                              |
|                |              |                                                                                 | In Airflow, steps can be anything, running locally in Airflow or remotely.  | https://airflow.apache.org/docs/apache-airflow/2.7.1/core-concepts/tasks.html        |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Language       | Language     | Programming or declarative language used to define pipelines and steps.         | In dbt, users write SQL, YML and Python to define the steps of a pipeline.  | https://docs.getdbt.com/docs/introduction#dbt-optimizes-your-workflow                |
|                |              |                                                                                 | Airflow expects steps and pipelines are written in Python.                  | https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html  |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Variables      | Variables    | Key-value configuration that can be used in steps and avoids hard-coded values  |                                                                             | https://docs.getdbt.com/docs/build/project-variables                                 |
|                |              |                                                                                 |                                                                             | https://airflow.apache.org/docs/apache-airflow/2.7.1/core-concepts/variables.html    |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Templating     | Macros       | Jinja templating used to access variables, configuration and reference steps    | dbt encourages using jinja templating for control structures (if and for).  | https://docs.getdbt.com/docs/build/jinja-macros                                      |
|                |              |                                                                                 | Native in Airflow/Python, used to define variables, macros and filters.     | https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html             |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Connection     | Profile      | Configuration to connect to databases or other services                         |                                                                             | https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html          |
|                |              |                                                                                 |                                                                             | https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles          |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| Providers      | Adapter      | Additional Python libraries that support specific databases or services         |                                                                             | https://airflow.apache.org/docs/apache-airflow-providers/                            |
|                |              |                                                                                 |                                                                             | https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/1-what-are-adapters |
+----------------+--------------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
