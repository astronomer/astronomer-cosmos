.. _execution-modes:

Execution Modes
===============

Cosmos can run ``dbt`` commands using four different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using a local ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands from Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands from Docker containers managed by Cosmos (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands from Kubernetes Pods managed by Cosmos (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary based on each user's needs and concerns. The default ``execution_mode`` is **local**. For more details, check each execution mode described below.

You should install Cosmos differently based on the ``execution mode``,  Learn more at :ref:`Installation Options <install-options>`.


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

Local
-----

By default, Cosmos uses the ``local`` execution mode.

The ``local`` execution mode is the fastest way to run Cosmos operators since they don't install ``dbt`` nor build docker containers. However, it may not be an option for users using managed Airflow services such as
Google Cloud Composer, since Airflow and ``dbt`` dependencies can conflict, the user may not be able to install ``dbt`` in a custom path.

The ``local`` execution mode assumes a ``dbt`` binary is reachable within the Airflow worker node.

If ``dbt`` was not `installed as part of the Cosmos package <install-options.html#local>`__,
users can define a custom path to ``dbt`` by declaring the argument ``dbt_executable_path``.

When using the ``local`` execution mode, Cosmos converts Airflow Connections into a native ``dbt`` profiles file (``profiles.yml``).

Example of how to use, for instance, when ``dbt`` was installed together with Cosmos:

    .. literalinclude:: ../../dev/dags/basic_cosmos_dag.py
       :language: python
       :start-after: [START local_example]
       :end-before: [END local_example]

Detailed examples of how to use the ``local`` execution mode when ``dbt`` is installed separately from Cosmos:

* `Astro <execution-mode-local-in-astro.html>`__
* `Docker <execution-mode-local-in-docker.html>`__
* `MWAA <execution-mode-local-in-mwaa.html>`__

Virtualenv
----------

If you're using managed Airflow on GCP (Cloud Composer), for instance,
we recommend you use the ``virtualenv`` execution mode.

The ``virtualenv`` mode isolates the Airflow worker dependencies from ``dbt`` by managing a Python virtual environment created
during task execution and deleted afterwards. In this case, users are responsible for declaring which version of ``dbt`` they
want to use using the argument ``py_requirements``.

In this case, users are responsible for declaring which version of ``dbt`` they want to use by giving the argument ``py_requirements``. This argument can be set directly in operator instances or when instantiating ``DbtDag`` and ``DbtTaskGroup`` as part of ``operator_args``.

Similar to the ``local`` execution mode, Cosmos converts Airflow Connections into a way ``dbt`` understands them by creating
a ``dbt`` profile file (``profiles.yml``).

A drawback with this approach is that it is slower than ``local`` because it creates a new Python virtual environment for each Cosmos dbt task run.

Example of how to use:

.. literalinclude:: ../../dev/dags/example_virtualenv.py
   :language: python
   :start-after: [START virtualenv_example]
   :end-before: [END virtualenv_example]

Docker
------

The ``docker`` approach assumes users have a previously created Docker image, which should contain all the ``dbt`` pipelines and
a ``profiles.yml``, managed by the user.

The user has better environment isolation than when using ``local`` or ``virtualenv`` modes, but also more responsibility (ensuring the Docker container used has up-to-date files and managing secrets potentially in multiple places).

The other challenge with the ``docker`` approach is if the Airflow worker is already running in Docker,
which sometimes can lead to challenges running `Docker in Docker <https://devops.stackexchange.com/questions/676/why-is-docker-in-docker-considered-bad>`__.

This approach can be significantly slower than ``virtualenv`` since it may have to build the ``Docker`` container,
which is slower than creating a Virtualenv with ``dbt-core``.

Check the step-by-step guide on using the ``docker`` execution mode at :ref:`Docker operators <execution-mode-docker>`.

Example DAG:

.. code-block:: python

  docker_cosmos_dag = DbtDag(
      # ...
      execution_mode="docker",
      operator_args={
          "image": "dbt-jaffle-shop:1.0.0",
          "network_mode": "bridge",
      },
  )


Kubernetes
----------

Lastly, the ``kubernetes`` approach is the most isolated way of running ``dbt`` since the ``dbt`` run commands
from within a Kubernetes Pod, usually in a separate host.

It assumes the user has a Kubernetes cluster. It also expects the user to ensure the Docker container has up-to-date ``dbt`` pipelines and profiles, potentially leading the user to declare secrets in two places (Airflow and Docker container).

The ``Kubernetes`` deployment may be slower than ``Docker`` and ``Virtualenv`` assuming that the container image is built (which is slower than creating a Python ``virtualenv`` and installing ``dbt-core``) and the Airflow task needs to spin up a new ``Pod`` in Kubernetes.

Check the step-by-step guide on using the ``kubernetes`` execution mode at :ref:`Kubernetes Operators <execution-mode-kubernetes>`.

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
        execution_mode="kubernetes",
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [postgres_password_secret],
        },
    )
