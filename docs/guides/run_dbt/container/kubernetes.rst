.. _kubernetes:


Kubernetes execution mode
=========================

The ``kubernetes`` execution mode provides a very isolated method to run ``dbt`` from within a Kubernetes Pod, usually in a separate host.

Performance and maintenance considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This execution mode assumes you have a Kubernetes cluster. It also expects you to ensure the Docker container has up-to-date ``dbt`` pipelines and profiles, potentially leading you to declare secrets in two places; `Apache Airflow® <https://airflow.apache.org/>`_ and Docker container.

The ``Kubernetes`` deployment might be slower than ``Docker`` and ``Virtualenv``, assuming that the container image is built (which is slower than creating a Python ``virtualenv`` and installing ``dbt-core``) and the Airflow task needs to spin up a new ``Pod`` in Kubernetes.


Set up Kubernetes execution mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following tutorial illustrates how to run the Cosmos dbt Kubernetes Operator using a local Kubernetes (K8s) cluster. It assumes the following:

- Postgres is run in the Kubernetes (K8s) cluster as a container
- Airflow is run locally, and it triggers a K8s Pod which runs dbt

Requirements
~~~~~~~~~~~~

To test the DbtKubernetesOperators locally, we encourage you to install the following:

- Local Airflow (either standalone or using Astro CLI)
- `Kind <https://kind.sigs.k8s.io/>`_ to run K8s locally
- `Helm <https://helm.sh/docs/helm/helm_install/>`_ to install Postgres in K8s
- `Docker <https://docs.docker.com/get-docker/>`_ to create the dbt container image, which will allow Airflow to create a K8s pod which will run dbt

At the moment, the user is expected to add to the Docker image both:

- The dbt project files
- The dbt Profile, which contains the information for dbt to access the database while parsing the project from Apache Airflow nodes
- Handle secrets

If you plan to generate dbt docs and upload them to S3 from Kubernetes, the image also needs the AWS CLI because Cosmos performs the upload from inside the Pod.

Additional KubernetesPodOperator parameters can be added to the ``operator_args`` parameter of the ``DbtKubernetesOperator``.

For instance,

.. literalinclude:: ../../../../dev/dags/jaffle_shop_kubernetes.py
   :language: python
   :start-after: [START kubernetes_tg_example]
   :end-before: [END kubernetes_tg_example]

To generate dbt docs and upload them to S3 from the same Pod, use :class:`~cosmos.operators.kubernetes.DbtDocsS3KubernetesOperator` and Cosmos 1.15.0 or higher.
See :doc:`../../dbt_docs/generating-docs` for an end-to-end example and the extra requirements for this workflow.

Container command and image ENTRYPOINT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Cosmos leaves the Pod's ``cmds`` unset and passes the full ``dbt`` command (including the ``dbt`` executable) through the Pod's ``arguments``. This preserves the container image's ``ENTRYPOINT``, so images whose ``ENTRYPOINT`` performs setup before ``dbt`` runs (for example, sourcing secrets injected by a sidecar such as a Vault agent) continue to work.

If you need to run ``dbt`` under a specific container command, set ``cmds`` via ``operator_args``. Cosmos handles the value you provide as follows:

- When ``cmds`` is left unset (the default), the image ``ENTRYPOINT`` runs and the full ``dbt`` command is passed as ``arguments``.
- When ``cmds`` is set to ``["dbt"]``, Cosmos runs ``dbt`` as the container command and passes the remaining tokens as ``arguments``.
- When ``cmds`` is set to a custom wrapper (for example ``["/custom-entrypoint.sh"]``), Cosmos preserves it as the container command and passes the full ``dbt`` command, including the executable, as ``arguments``. The wrapper is responsible for invoking ``dbt`` with those arguments.

.. note::

    If your image's ``ENTRYPOINT`` is itself ``dbt`` (that is, ``["dbt"]``), leaving ``cmds`` unset makes Kubernetes run ``ENTRYPOINT`` followed by ``arguments``, which results in ``dbt dbt ...``. For such images, set ``cmds=["dbt"]`` so Cosmos strips the leading executable from ``arguments``.

For example, to run ``dbt`` through a custom entrypoint script baked into your image:

.. code-block:: python

    DbtKubernetesOperator(
        ...,
        operator_args={"cmds": ["/custom-entrypoint.sh"]},
    )

This behavior is shared by the ``kubernetes``, ``watcher_kubernetes``, ``aws_eks``, and ``gcp_gke`` execution modes, which all build their Pod command the same way.

Behavior across versions
++++++++++++++++++++++++

- In Cosmos versions before 1.15.0, Cosmos did not set ``cmds`` on the Pod. The image ``ENTRYPOINT`` was preserved, and any ``cmds`` supplied through ``operator_args`` was used as provided.
- In Cosmos 1.15.0, Cosmos began setting ``cmds`` to ``["dbt"]`` unconditionally. Because setting the Pod ``cmds`` overrides the container ``command``, this overrode the image ``ENTRYPOINT`` and also discarded any custom ``cmds`` value (`#2889 <https://github.com/astronomer/astronomer-cosmos/issues/2889>`__).
- Cosmos 1.15.1 restores the earlier behavior: the image ``ENTRYPOINT`` is preserved by default, a custom ``cmds`` is honored as provided, and ``["dbt"]`` is used as the container command only when set explicitly.

Step-by-step instructions
~~~~~~~~~~~~~~~~~~~~~~~~~

Using installed `Kind <https://kind.sigs.k8s.io/>`_, you can setup a local kubernetes cluster

.. code-block:: bash

    kind create cluster

Deploy a Postgres pod to Kind using `Helm <https://helm.sh/docs/helm/helm_install/>`_

.. code-block:: bash

    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    helm install postgres bitnami/postgresql

Retrieve the Postgres password and set it as an environment variable.

.. code-block:: bash

    export POSTGRES_PASSWORD=$(kubectl get secret --namespace default postgres-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

Check that the environment variable was set and that it is not empty

.. code-block:: bash

    echo $POSTGRES_PASSWORD

Expose the Postgres to the host running Docker/Kind.

.. code-block:: bash

    kubectl port-forward --namespace default postgres-postgresql-0  5432:5432

Check that you're able to connect to the exposed pod.

.. code-block:: bash

    PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U postgres -d postgres -p 5432

    postgres=# \dt
    \q

Create a K8s secret which contains the credentials to access Postgres.

.. code-block:: bash

    kubectl create secret generic postgres-secrets --from-literal=host=postgres-postgresql.default.svc.cluster.local --from-literal=password=$POSTGRES_PASSWORD

Clone the example repo that contains the Airflow DAG and dbt project files.

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-example.git
    cd cosmos-example/

Create a Docker image containing the dbt project files and dbt profile by using the `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/Dockerfile.postgres_profile_docker_k8s>`_, which will be run in K8s.

.. code-block:: bash

    docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .

.. note::

    If running on M1, you may need to set the following environment variables to run the Docker build command in case it fails.

    .. code-block:: bash

        export DOCKER_BUILDKIT=0
        export COMPOSE_DOCKER_CLI_BUILD=0
        export DOCKER_DEFAULT_PLATFORM=linux/amd64

Take a look at the Dockerfile to understand its purpose so that you can use it as a reference in your project.

    - The `dbt profile <https://github.com/astronomer/cosmos-example/blob/main/example_postgres_profile.yml>`__ file is added to the image
    - The dags directory containing the `dbt project jaffle_shop <https://github.com/astronomer/cosmos-example/tree/main/dags/dbt/jaffle_shop>`_ is added to the image
    - The dbt_project.yml is replaced with `postgres_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/postgres_profile_dbt_project.yml>`_ which contains the profile key pointing to postgres_profile as profile creation is not handled at the moment for K8s operators like in local mode.

Make the build image available in the Kind K8s cluster.

.. code-block:: bash

    kind load docker-image dbt-jaffle-shop:1.0.0

Create a Python virtual environment and install the latest version of Astronomer Cosmos, which contains the K8s Operator.

.. code-block:: bash

    python -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install "astronomer-cosmos[dbt-postgres]" apache-airflow-providers-cncf-kubernetes

Make the `jaffle_shop_kubernetes.py <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/jaffle_shop_kubernetes.py>`__ file at your Airflow DAG home:

.. code-block:: bash

    cp -r dags $AIRFLOW_HOME/

Run Airflow

.. code-block:: bash

    airflow standalone

.. note::

    You may need to run Airflow standalone with ``sudo`` if your Airflow user is unable to access the Docker socket URL or pull images in the Kind cluster.

Log in to Airflow through a web browser ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

Enable and trigger a run of the `jaffle_shop_k8s <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_kubernetes.py>`_ DAG. You will be able to see the following successful DAG run.

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_k8s_dag_run.png
    :width: 800

.. _kubernetes-known-limitations:

Known limitations
~~~~~~~~~~~~~~~~~

The Kubernetes execution mode has the following limitations:

- Does not emit OpenLineage events (there is an `open ticket #496 <https://github.com/astronomer/astronomer-cosmos/issues/496>`__ to address this)
- Does not emit Airflow datasets, assets, and dataset aliases (there is an `open ticket #2329 <https://github.com/astronomer/astronomer-cosmos/issues/2329>`__ to address this)
- Does not handle installing dbt deps for users (there is an `open ticket #679 <https://github.com/astronomer/astronomer-cosmos/issues/679>`__ to address this)
- Does not support `ProfileMapping <https://astronomer.github.io/astronomer-cosmos/guides/connect_database/use-profile-mapping.html>`_ (there is an `open ticket #749 <https://github.com/astronomer/astronomer-cosmos/issues/749>`__ to address this)
- Does not support :doc:`../callbacks/callbacks` (there is an `open ticket #1575 <https://github.com/astronomer/astronomer-cosmos/issues/1575>`__ to address this)
- Does not expose Compiled SQL as a `templated field <https://astronomer.github.io/astronomer-cosmos/guides/cosmos_devex/compiled-sql.html>`_
- Does not benefit from `Cosmos caching mechanisms <https://astronomer.github.io/astronomer-cosmos/optimize_performance/caching.html>`_
- Since 1.15.0, supports generating dbt docs and uploading them to S3 with :class:`~cosmos.operators.kubernetes.DbtDocsS3KubernetesOperator`; other object stores and callback-based uploads remain unsupported in Kubernetes execution mode
