.. _execution-modes:

Execution Modes
===============

Cosmos can run DBT commands using four different approaches, called ``execution modes``:

1. **local**: Run ``dbt`` commands using an user-managed ``dbt`` installation (default)
2. **virtualenv**: Run ``dbt`` commands using Python virtual environments managed by Cosmos
3. **docker**: Run ``dbt`` commands using Docker (requires a pre-existing Docker image)
4. **kubernetes**: Run ``dbt`` commands within a Kubernetes Pod (requires a pre-existing Docker image)

The choice of the ``execution mode`` can vary depending on each user's needs and concerns.

Based on the ``execution mode``, Cosmos should be installed in different ways.
Learn more at :ref:`Installation Options <install-options>`.

The default ``execution_mode`` is **local**.

If using a cloud provider, the recommendation is to use the **virtualenv** execution mode.

For more details, check each execution mode description below.

.. list-table:: Execution Modes Comparison
   :widths: 25 25 25 25
   :header-rows: 1

   * - Execution Mode
     - Task Duration
     - Environment Isolation
     - Cosmos Profile Management
   * - Local
     - Fastest
     - None
     - Yes
   * - Virtualenv
     - Fast
     - Lightweight
     - Yes
   * - Docker
     - Medium
     - Medium
     - No
   * - Kubernetes
     - Medium
     - High
     - No

Local
-----

 from the same environment as Airflow  (optional: define the path to a dbt binary)

Although ``local`` is the fastest way to run Cosmos operators, it also assumes that the DBT Python dependencies do not
conflict with the Airflow worker dependencies - which is often not true, specially when running in some Cloud managed services.

When using the ``local`` execution mode, Cosmos converts Airflow Connections into a way DBT understands them by creating a
DBT profile file (``profiles.yml``).

Example of how to use:

    .. literalinclude:: ../../dev/dags/basic_cosmos_dag.py
       :language: python
       :start-after: [START local_example]
       :end-before: [END local_example]


The ``local`` execution mode also allows users to declare the path to a custom ``dbt`` binary path, by setting the argument ``dbt_executable_path``.
In this case, the user is responsible for pre-installing DBT (potentially in a user-maintained virtual environment) and manage its extensions.

Virtualenv
----------

If you're using managed Airflow solutions on AWS (Amazon MWAA), Azure (Azure Data Factory's Managed Airflow) and GCP (Cloud Composer),
we recommend you use the ``virtualenv`` execution mode.

The ``virtualenv`` mode isolates the Airflow worker dependencies from DBT by managing a Python virtual environment created
during task execution and deleted afterwards. In this case, users are responsible for declaring which version of DBT they
want to use by utilizing the argument ``py_requirements``.

In this case, users are responsible for declaring which version of DBT they
want to use by utilizing the argument ``py_requirements``. This value can be set when instantiating operators directly
or creating instances of ``DbtDag`` or ``DbtTaskGroup`` from within the parameter ``operator_args``.

Similar to the ``local`` execution mode, Cosmos converts Airflow Connections into a way DBT understands them by creating
a DBT profile file (``profiles.yml``).

This approach is a bit slower than ``local``, because a new Python virtual environment is created each time a task is run.

A drawback with this approach is that everytime a task is run with Cosmos, a new Python ``virtualenv`` is created, which
may be slow depending on the user-defined dependencies.

Example of how to use:

    .. literalinclude:: ../../dev/dags/example_virtualenv.py
       :language: python
       :start-after: [START virtualenv_example]
       :end-before: [END virtualenv_example]

Docker
------

The ``docker`` approach assumes users have a previously created Docker image, which should contain the DBT pipelines and
a ``profiles.yml``, managed by the user.
The user has better environment isolation than when using ``local`` or ``virtualenv`` modes, but also more responsibility
(ensuring the Docker container used has the up-to-date files and managing secrets potentially in multiple places).
The other challenge with the ``docker`` approach is if the Airflow worker is already running in Docker,
which sometimes can lead to challenges running Docker in Docker.

Check the step-by-step guide on how to user the ``docker`` execution mode at ::ref:`Execution Mode Docker <execution-mode-docker>`.

Example DAG:

.. code-block:: python

  docker_cosmos_dag = DbtDag(
        (...)
        execution_mode="docker",
        operator_args={
            "image": "dbt-jaffle-shop:1.0.0",
            "network_mode": "bridge",
        }
  )


Kubernetes
----------

Lastly, the ``kubernetes`` approach is the most isolated way of running DBT, since not only the DBT commands are run
from within a container, but also potentially in a separate host/pod.

It assumes the user has a Kubernetes cluster.

It also expects the user has to ensure the Docker container has up-to-date pipeline and DBT profiles,
potentially leading the user to declare secrets in two different places (Airflow and Docker container).

Check the step-by-step guide on how to user the ``docker`` execution mode at ::ref:`Execution Mode Kubernetes <execution-mode-kubernetes>`.

Example DAG:

.. code-block:: python

    postgres_password_secret = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_PASSWORD",
        secret="postgres-secrets",
        key="password",
    )

    docker_cosmos_dag = DbtDag(
          (...)
          execution_mode="kubernetes",
          operator_args={
              "image": "dbt-jaffle-shop:1.0.0",
              "get_logs": True,
              "is_delete_operator_pod": False,
              "secrets": [postgres_password_secret]
    )
