.. _getting-started:

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Cosmos Fundamentals

   Similar dbt and Airflow concepts <dbt-airflow-concepts>

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Quickstart

   Astro CLI quickstart <astro-cli-quickstart>

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Get started with Cosmos

   Open-source Airflow <open-source>
   Astro <astro>
   Amazon Managed Workflows for Apache Airflow (MWAA) <mwaa>
   Google Cloud Composer (GCC) <gcc>


Getting Started
===============

The recommended way to install and run Cosmos depends on how you run Airflow. For specific guides, see the following:

- `Getting Started on Open-Source <open-source.html>`__
- `Getting Started on Astro <astro.html>`__
- `Getting Started on MWAA <mwaa.html>`__
- `Getting Started on GCC <gcc.html>`__

You might require a different setup depending on your particular configuration. See :ref:`exec-methods`.

Example Demo: Jaffle Shop Project
__________________________________

You can explore a practical example in the **Getting Started** guides to see how Cosmos can convert the dbt workflow into an Airflow Dag.

The `jaffle_shop project <https://github.com/dbt-labs/jaffle_shop>`_ is a sample dbt project that simulates an e-commerce store's data.
The project includes a series of dbt models that transform raw data into structured tables, such as sales, customers, and products.

The following diagram shows the original dbt workflow in a lineage graph. This graph illustrates the relationships between different models:

.. image:: /_static/jaffle_shop_dbt_graph.png

Cosmos can take this dbt workflow and convert it into an Airflow Dag, allowing you to leverage Airflow's scheduling and
orchestration capabilities.

To convert this dbt workflow into an Airflow Dag, create a new Dag definition file, import ``DbtDag`` from the Cosmos library,
and fill in a few parameters, such as the dbt project directory path and the profile name:

..
   The following renders in Sphinx but not Github:

.. literalinclude:: ./../../dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]


This code snippet then generates an Airflow Dag like this:

.. image:: https://raw.githubusercontent.com/astronomer/astronomer-cosmos/main/docs/_static/jaffle_shop_dag.png

``DbtDag`` is a custom Dag generator that converts dbt projects into Airflow Dags. It also accepts Cosmos-specific arguments like
``fail_fast``, to immediately fail a dag if dbt fails to process a resource, or ``cancel_query_on_kill``, to cancel any running
queries if the task is externally killed or manually set to failed in Airflow. ``DbtDag`` also accepts standard Dag arguments such
as ``max_active_tasks``, ``max_active_runs``, and ``default_args``.

With Cosmos, transitioning from a dbt workflow to an Airflow Dag is seamless, giving you the best of both tools
for managing and scaling your data workflows.

.. _exec-methods:

Execution Methods
-----------------

For more customization, check out the different execution modes that Cosmos supports on the `Execution Modes <execution-modes.html>`__ page.

For specific guides, see the following:

- `Executing dbt DAGs with DockerOperators <../../guides/run_dbt/container/docker.html>`__
- `Executing dbt DAGs with KubernetesPodOperators <../../guides/run_dbt/container/kubernetes.html>`__
- `Executing dbt DAGs with Watcher Kubernetes Mode <../../guides/run_dbt/container/watcher-kubernetes-execution-mode.html>`__
- `Executing dbt DAGs with AzureContainerInstancesOperators <../../guides/run_dbt/container/azure-container-instance.html>`__
- `Executing dbt DAGs with GcpCloudRunExecuteJobOperators <../../guides/run_dbt/container/gcp-cloud-run-job.html>`__


Concepts Overview
-----------------

How do dbt and Airflow concepts map to each other? Learn more `in this link <dbt-airflow-concepts.html>`__.
