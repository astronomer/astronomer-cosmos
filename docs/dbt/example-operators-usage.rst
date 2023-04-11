Example Operators Usage
=======================

Follow the instructions below to setup your environment as a reference for the operator(s) you would need to use in your DAG.

.. tabs::

    .. tab:: Kubernetes Operators

        The following tutorial illustrates how to run the Cosmos DBT Kubernetes Operator using a local K8s cluster. It assumes the following:

        - Postgres is run in the Kubernetes (K8s) cluster as a container
        - Airflow is run locally, and it triggers a K8s Pod which runs DBT

        **Requirements**

        To test the DbtKubernetesOperators locally, we encourage you to install the following:

        - Local Airflow (either standalone or using Astro CLI)
        - `Kind <https://kind.sigs.k8s.io/>`_ to run K8s locally
        - `Helm <https://helm.sh/docs/helm/helm_install/>`_ to install Postgres in K8s
        - `Docker <https://docs.docker.com/get-docker/>`_ to create the DBT container image, which will allow Airflow to create a K8s pod which will run DBT

        At the moment, the user is expected to add to the Docker image both:

        - The DBT project files
        - The DBT Profile which contains the information for DBT to access the database
        - Handle secrets

        **Step-by-step instructions**

        Using installed `Kind <https://kind.sigs.k8s.io/>`_, you can setup a local kubernetes cluster

        .. code-block:: bash

            kind cluster create

        Deploy a Postgres pod to Kind using `Helm <https://helm.sh/docs/helm/helm_install/>`_

        .. code-block:: bash

            helm repo add bitnami https://charts.bitnami.com/bitnami
            helm repo update
            helm install postgres bitnami/postgresql

        Retrieve the Postgres password and set it as an environment variable

        .. code-block:: bash

            export POSTGRES_PASSWORD=$(kubectl get secret --namespace default postgres-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

        Check that the environment variable was set and that it is not empty

        .. code-block:: bash

            echo $POSTGRES_PASSWORD

        Expose the Postgres to the host running Docker/Kind

        .. code-block:: bash

            kubectl port-forward --namespace default postgres-postgresql-0  5432:5432

        Check that you're able to connect to the exposed pod

        .. code-block:: bash

            PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U postgres -d postgres -p 5432

            postgres=# \dt
            \q

        Create a K8s secret which contains the credentials to access Postgres

        .. code-block:: bash

            kubectl create secret generic postgres-secrets --from-literal=host=postgres-postgresql.default.svc.cluster.local --from-literal=password=$POSTGRES_PASSWORD

        Clone the example repo that contains the Airflow DAG and DBT project files

        .. code-block:: bash

            git clone https://github.com/astronomer/cosmos-example.git
            cd cosmos-example/

        Create a docker image containing the DBT project files and DBT profile by using the `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/Dockerfile.postgres_profile_docker_k8s>`_, which will be run in K8s.

        .. code-block:: bash

            docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .

        .. note::

            If running on M1, you may need to set the following envvars for running the docker build command in case it fails

            .. code-block:: bash

                export DOCKER_BUILDKIT=0
                export COMPOSE_DOCKER_CLI_BUILD=0
                export DOCKER_DEFAULT_PLATFORM=linux/amd64

        Take a read of the Dockerfile to understand what it does so that you could use it as a reference in your project.

            - The `DBT profile <https://github.com/astronomer/cosmos-example/blob/main/example_postgres_profile.yml>`_ file is added to the image
            - The dags directory containing the `DBT project jaffle_shop <https://github.com/astronomer/cosmos-example/tree/main/dags/dbt/jaffle_shop>`_ is added to the image
            - The dbt_project.yml is replaced with `postgres_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/postgres_profile_dbt_project.yml>`_ which contains the profile key pointing to postgres_profile as profile creation is not handled at the moment for K8s operators like in local mode.

        Make the build image available in the Kind K8s cluster

        .. code-block:: bash

            kind load docker-image dbt-jaffle-shop:1.0.0

        Create a Python virtual environment and install the latest version of Astronomer Cosmos which contains the K8s Operator

        .. code-block:: bash

            python -m venv venv
            source venv/bin/activate
            pip install --upgrade pip
            pip install astronomer-cosmos[dbt-postgres]

        Copy the dags directory from cosmos-example repo to your Airflow home

        .. code-block:: bash

            cp -r dags $AIRFLOW_HOME/

        Run Airflow

        .. code-block:: bash

            airflow standalone

        .. note::

            You might need to run airflow standalone with ``sudo`` if your Airflow user is not able to access the docker socket URL or pull the images in the Kind cluster.

        Log in to Airflow through a web browser ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

        Enable and trigger a run of the `jaffle_shop_k8s <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_kubernetes.py>`_ DAG. You will be able to see the following successful DAG run.

        .. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_k8s_dag_run.png
           :width: 800
















    .. tab:: Docker Operators
