.. _getting-started:

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Cosmos Fundamentals

   Cosmos fundamentals <cosmos-fundamentals>
   Similar dbt and Airflow <dbt-airflow-concepts>

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Get started with Cosmos

   Run Cosmos <run-cosmos>
   astro
   aws-container-run-job
   gcc
   mwaa
   open-source

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Execution Modes

   Execution Modes <execution-modes>
   execution-modes-local-conflicts
   Docker Execution Mode <docker>
   Kubernetes Execution Mode <kubernetes>
   Azure Container Instance Execution Mode <azure-container-instance>
   AWS Container Run Job Execution Mode <aws-container-run-job>
   GCP Cloud Run Job Execution Mode <gcp-cloud-run-job>
   Airflow Async Execution Mode <async-execution-mode>
   Watcher Execution Mode <watcher-execution-mode>
   Watcher Kubernetes Execution Mode <watcher-kubernetes-execution-mode>

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption:
   Operators <operators>
   Custom Airflow Properties <custom-airflow-properties>

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

- `Executing dbt DAGs with Docker Operators <docker.html>`__
- `Executing dbt DAGs with KubernetesPodOperators <kubernetes.html>`__
- `Executing dbt DAGs with Watcher Kubernetes Mode <watcher-kubernetes-execution-mode.html>`__
- `Executing dbt DAGs with AzureContainerInstancesOperators <azure-container-instance.html>`__
- `Executing dbt DAGs with GcpCloudRunExecuteJobOperators <gcp-cloud-run-job.html>`__


Concepts Overview
-----------------

How do dbt and Airflow concepts map to each other? Learn more `in this link <dbt-airflow-concepts.html>`__.
