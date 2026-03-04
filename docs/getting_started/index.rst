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
   Google Cloud Composer (GCC) <gcc>
   Amazon Managed Workflows for Apache Airflow (MWAA) <mwaa>

Getting Started
===============

Recommended Methods
-------------------

The recommended way to install and run Cosmos depends on how you run Airflow. For specific guides, see the following:

- `Getting Started on Astro <astro.html>`__
- `Getting Started on MWAA <mwaa.html>`__
- `Getting Started on GCC <gcc.html>`__
- `Getting Started on Open-Source <open-source.html>`__

While the above methods are recommended, you may require a different setup. Check out the sections below for more information.


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
