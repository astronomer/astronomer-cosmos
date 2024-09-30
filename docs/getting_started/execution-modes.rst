.. _execution-modes:

Execution Modes
===============

Cosmos can run ``dbt`` commands using five different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using a local ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands from Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands from Docker containers managed by Cosmos (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands from Kubernetes Pods managed by Cosmos (requires a pre-existing Docker image)
5. **aws_eks**: Run ``dbt`` commands from AWS EKS Pods managed by Cosmos (requires a pre-existing Docker image)
6. **azure_container_instance**: Run ``dbt`` commands from Azure Container Instances managed by Cosmos (requires a pre-existing Docker image)
7. **gcp_cloud_run_job**: Run ``dbt`` commands from GCP Cloud Run Job instances managed by Cosmos (requires a pre-existing Docker image)
8. **airflow_async**: (Introduced since Cosmos 1.7.0) Run the dbt resources from your dbt project asynchronously, by submitting the corresponding compiled SQLs to Apache Airflow's `Deferrable operators <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`__

The choice of the ``execution mode`` can vary based on each user's needs and concerns. For more details, check each execution mode described below.


.. list-table:: Execution Modes Comparison
   :widths: 25 25 25 25
   :header-rows: 1

   * - Execution Mode
     - Task Duration
     - Environment Isolation
     - Cosmos Profile Management
   * - Local
     - Fast
     - None
     - Yes
   * - Virtualenv
     - Medium
     - Lightweight
     - Yes
   * - Docker
     - Slow
     - Medium
     - No
   * - Kubernetes
     - Slow
     - High
     - No
   * - AWS_EKS
     - Slow
     - High
     - No
   * - Azure Container Instance
     - Slow
     - High
     - No
   * - GCP Cloud Run Job Instance
     - Slow
     - High
     - No
   * - Airflow Async
     - Medium
     - None
     - Yes

Local
-----

By default, Cosmos uses the ``local`` execution mode.

The ``local`` execution mode is the fastest way to run Cosmos operators since they don't install ``dbt`` nor build docker containers. However, it may not be an option for users using managed Airflow services such as
Google Cloud Composer, since Airflow and ``dbt`` dependencies can conflict (:ref:`execution-modes-local-conflicts`), the user may not be able to install ``dbt`` in a custom path.

The ``local`` execution mode assumes a ``dbt`` binary is reachable within the Airflow worker node.

If ``dbt`` was not installed as part of the Cosmos packages,
users can define a custom path to ``dbt`` by declaring the argument ``dbt_executable_path``.

.. note::
    Starting in the 1.4 version, Cosmos tries to leverage the dbt partial parsing (``partial_parse.msgpack``) to speed up task execution.
    This feature is bound to `dbt partial parsing limitations <https://docs.getdbt.com/reference/parsing#known-limitations>`_.
    Learn more: :ref:`partial-parsing`.

When using the ``local`` execution mode, Cosmos converts Airflow Connections into a native ``dbt`` profiles file (``profiles.yml``).

Example of how to use, for instance, when ``dbt`` was installed together with Cosmos:

.. literalinclude:: ../../dev/dags/basic_cosmos_dag.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]


Virtualenv
----------

If you're using managed Airflow on GCP (Cloud Composer), for instance, we recommend you use the ``virtualenv`` execution mode.

The ``virtualenv`` mode isolates the Airflow worker dependencies from ``dbt`` by managing a Python virtual environment created during task execution and deleted afterwards.

In this case, users are responsible for declaring which version of ``dbt`` they want to use by giving the argument ``py_requirements``. This argument can be set directly in operator instances or when instantiating ``DbtDag`` and ``DbtTaskGroup`` as part of ``operator_args``.

Similar to the ``local`` execution mode, Cosmos converts Airflow Connections into a way ``dbt`` understands them by creating a ``dbt`` profile file (``profiles.yml``).
Also similar to the ``local`` execution mode, Cosmos will by default attempt to use a ``partial_parse.msgpack`` if one exists to speed up parsing.

Some drawbacks of this approach:

- It is slower than ``local`` because it creates a new Python virtual environment for each Cosmos dbt task run.
- If dbt is unavailable in the Airflow scheduler, the default ``LoadMode.DBT_LS`` will not work. In this scenario, users must use a `parsing method <parsing-methods.html>`_  that does not rely on dbt, such as ``LoadMode.MANIFEST``.
- Only ``InvocationMode.SUBPROCESS`` is supported currently, attempt to use ``InvocationMode.DBT_RUNNER`` will raise error.

Example of how to use:

.. literalinclude:: ../../dev/dags/example_virtualenv.py
   :language: python
   :start-after: [START virtualenv_example]
   :end-before: [END virtualenv_example]

Docker
------

The ``docker`` approach assumes users have a previously created Docker image, which should contain all the ``dbt`` pipelines and a ``profiles.yml``, managed by the user.

The user has better environment isolation than when using ``local`` or ``virtualenv`` modes, but also more responsibility (ensuring the Docker container used has up-to-date files and managing secrets potentially in multiple places).

The other challenge with the ``docker`` approach is if the Airflow worker is already running in Docker, which sometimes can lead to challenges running `Docker in Docker <https://devops.stackexchange.com/questions/676/why-is-docker-in-docker-considered-bad>`__.

This approach can be significantly slower than ``virtualenv`` since it may have to build the ``Docker`` container, which is slower than creating a Virtualenv with ``dbt-core``.
If dbt is unavailable in the Airflow scheduler, the default ``LoadMode.DBT_LS`` will not work. In this scenario, users must use a `parsing method <parsing-methods.html>`_  that does not rely on dbt, such as ``LoadMode.MANIFEST``.

Check the step-by-step guide on using the ``docker`` execution mode at :ref:`docker`.

Example DAG:

.. code-block:: python

  docker_cosmos_dag = DbtDag(
      # ...
      execution_config=ExecutionConfig(
          execution_mode=ExecutionMode.DOCKER,
      ),
      operator_args={
          "image": "dbt-jaffle-shop:1.0.0",
          "network_mode": "bridge",
      },
  )


Kubernetes
----------

The ``kubernetes`` approach is a very isolated way of running ``dbt`` since the ``dbt`` run commands from within a Kubernetes Pod, usually in a separate host.

It assumes the user has a Kubernetes cluster. It also expects the user to ensure the Docker container has up-to-date ``dbt`` pipelines and profiles, potentially leading the user to declare secrets in two places (Airflow and Docker container).

The ``Kubernetes`` deployment may be slower than ``Docker`` and ``Virtualenv`` assuming that the container image is built (which is slower than creating a Python ``virtualenv`` and installing ``dbt-core``) and the Airflow task needs to spin up a new ``Pod`` in Kubernetes.

Check the step-by-step guide on using the ``kubernetes`` execution mode at :ref:`kubernetes`.

Example DAG:

.. literalinclude:: ../../dev/dags/jaffle_shop_kubernetes.py
   :language: python
   :start-after: [START kubernetes_seed_example]
   :end-before: [END kubernetes_seed_example]

AWS_EKS
----------

The ``aws_eks`` approach is very similar to the ``kubernetes`` approach, but it is specifically designed to run on AWS EKS clusters.
It uses the `EKSPodOperator <https://airflow.apache.org/docs/apache-airflow-providers-amazon/8.19.0/operators/eks.html#perform-a-task-on-an-amazon-eks-cluster>`_
to run the dbt commands. You need to provide the ``cluster_name`` in your operator_args to connect to the AWS EKS cluster.


Example DAG:

.. code-block:: python

    postgres_password_secret = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_PASSWORD",
        secret="postgres-secrets",
        key="password",
    )

    docker_cosmos_dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.AWS_EKS,
        ),
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "cluster_name": CLUSTER_NAME,
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [postgres_password_secret],
        },
    )

Azure Container Instance
------------------------
.. versionadded:: 1.4
Similar to the ``kubernetes`` approach, using ``Azure Container Instances`` as the execution mode gives a very isolated way of running ``dbt``, since the ``dbt`` run itself is run within a container running in an Azure Container Instance.

This execution mode requires the user has an Azure environment that can be used to run Azure Container Groups in (see :ref:`azure-container-instance` for more details on the exact requirements). Similarly to the ``Docker`` and ``Kubernetes`` execution modes, a Docker container should be available, containing the up-to-date ``dbt`` pipelines and profiles.

Each task will create a new container on Azure, giving full isolation. This, however, comes at the cost of speed, as this separation of tasks introduces some overhead. Please checkout the step-by-step guide for using Azure Container Instance as the execution mode


.. code-block:: python

    docker_cosmos_dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.AZURE_CONTAINER_INSTANCE
        ),
        operator_args={
            "ci_conn_id": "aci",
            "registry_conn_id": "acr",
            "resource_group": "my-rg",
            "name": "my-aci-{{ ti.task_id.replace('.','-').replace('_','-') }}",
            "region": "West Europe",
            "image": "dbt-jaffle-shop:1.0.0",
        },
    )

GCP Cloud Run Job
------------------------
.. versionadded:: 1.7
The ``gcp_cloud_run_job`` execution mode is particularly useful for users who prefer to run their ``dbt`` commands on Google Cloud infrastructure, taking advantage of Cloud Run's scalability, isolation, and managed service capabilities.

For the ``gcp_cloud_run_job`` execution mode to work, a Cloud Run Job instance must first be created using a previously built Docker container. This container should include the latest ``dbt`` pipelines and profiles. You can find more details in the `Cloud Run Job creation guide <https://cloud.google.com/run/docs/create-jobs>`__ .

This execution mode allows users to run ``dbt`` core CLI commands in a Google Cloud Run Job instance. This mode leverages the ``CloudRunExecuteJobOperator`` from the Google Cloud Airflow provider to execute commands within a Cloud Run Job instance, where ``dbt`` is already installed. Similarly to the ``Docker`` and ``Kubernetes`` execution modes, a Docker container should be available, containing the up-to-date ``dbt`` pipelines and profiles.

Each task will create a new Cloud Run Job execution, giving full isolation. The separation of tasks adds extra overhead; however, that can be mitigated by using the ``concurrency`` parameter in ``DbtDag``, which will result in parallelized execution of ``dbt`` models.


.. code-block:: python

    gcp_cloud_run_job_cosmos_dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.GCP_CLOUD_RUN_JOB),
        operator_args={
            "project_id": "my-gcp-project-id",
            "region": "europe-west1",
            "job_name": "my-crj-{{ ti.task_id.replace('.','-').replace('_','-') }}",
        },
    )

Airflow Async
-------------

.. versionadded:: 1.7.0

(**Experimental**)

The ``airflow_async`` execution mode is a way to run the dbt resources from your dbt project using Apache Airflow's
`Deferrable operators <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`__.
This execution mode could be preferred when you've long running resources and you want to run them asynchronously by
leveraging Airflow's deferrable operators. With that, you would be able to potentially observe higher throughput of tasks
as more dbt nodes will be run in parallel since they won't be blocking Airflow's worker slots.

In this mode, Cosmos adds a new operator, ``DbtCompileAirflowAsyncOperator``, as a root task in the DAG. The task runs
the ``dbt compile`` command on your dbt project which then outputs compiled SQLs in the project's target directory.
As part of the same task run, these compiled SQLs are then stored remotely to a remote path set using the
:ref:`remote_target_path` configuration. The remote path is then used by the subsequent tasks in the DAG to
fetch (from the remote path) and run the compiled SQLs asynchronously using e.g. the ``DbtRunAirflowAsyncOperator``.
You may observe that the compile task takes a bit longer to run due to the latency of storing the compiled SQLs remotely,
however, it is still a win as it is one-time overhead and the subsequent tasks run asynchronously utilising the Airflow's
deferrable operators and supplying to them those compiled SQLs.

Note that currently, the ``airflow_async`` execution mode has the following limitations and is released as Experimental:

1. Only supports the ``dbt resource type`` models to be run asynchronously using Airflow deferrable operators. All other resources are executed synchronously using dbt commands as they are in the ``local`` execution mode.
2. Only supports BigQuery as the target database. If a profile target other than BigQuery is specified, Cosmos will error out saying that the target database is not supported with this execution mode.

Example DAG:

.. literalinclude:: ../../dev/dags/simple_dag_async.py
   :language: python
   :start-after: [START airflow_async_execution_mode_example]
   :end-before: [END airflow_async_execution_mode_example]

.. _invocation_modes:
Invocation Modes
================
.. versionadded:: 1.4

For ``ExecutionMode.LOCAL`` execution mode, Cosmos supports two invocation modes for running dbt:

1. ``InvocationMode.SUBPROCESS``: In this mode, Cosmos runs dbt cli commands using the Python ``subprocess`` module and parses the output to capture logs and to raise exceptions.

2. ``InvocationMode.DBT_RUNNER``: In this mode, Cosmos uses the ``dbtRunner`` available for `dbt programmatic invocations <https://docs.getdbt.com/reference/programmatic-invocations>`__ to run dbt commands. \
   In order to use this mode, dbt must be installed in the same local environment. This mode does not have the overhead of spawning new subprocesses or parsing the output of dbt commands and is faster than ``InvocationMode.SUBPROCESS``. \
   This mode requires dbt version 1.5.0 or higher. It is up to the user to resolve :ref:`execution-modes-local-conflicts` when using this mode.

The invocation mode can be set in the ``ExecutionConfig`` as shown below:

.. code-block:: python

    from cosmos.constants import InvocationMode

    dag = DbtDag(
        # ...
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.DBT_RUNNER,
        ),
    )

If the invocation mode is not set, Cosmos will attempt to use ``InvocationMode.DBT_RUNNER`` if dbt is installed in the same environment as the worker, otherwise it will fall back to ``InvocationMode.SUBPROCESS``.
