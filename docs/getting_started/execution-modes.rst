.. _execution-modes:

Execution Modes
===============

Cosmos can run ``dbt`` commands using five different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using a local ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands from Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands from Docker containers managed by Cosmos (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands from Kubernetes Pods managed by Cosmos (requires a pre-existing Docker image)
5. **eks**: Run ``dbt`` commands from EKS Pods managed by Cosmos (requires a pre-existing Docker image)
6. **azure_container_instance**: Run ``dbt`` commands from Azure Container Instances managed by Cosmos (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary based on each user's needs and concerns. For more details, check each execution mode described below.


.. list-table:: Execution Modes Comparison
   :widths: 20 20 20 20 20
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
   * - EKS
     - Slow
     - High
     - No
   * - Azure Container Instance
     - Slow
     - High
     - No

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
            execution_mode=ExecutionMode.KUBERNETES,
        ),
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [postgres_password_secret],
        },
    )
EKS
----------

The ``eks`` approach is very similar to the ``kubernetes`` approach, but it is specifically designed to run on AWS EKS clusters.
It uses the [EKSPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/operators/eks.html#perform-a-task-on-an-amazon-eks-cluster)
to run the dbt commands. You need to provide the ``cluster_name`` in your operator_args to connect to the EKS cluster.


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
            execution_mode=ExecutionMode.EKS,
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
