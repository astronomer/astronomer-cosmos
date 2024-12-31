.. _azure-container-instance:

Azure Container Instance Execution Mode
=======================================
.. versionadded:: 1.4

This tutorial will guide you through the steps required to use Azure Container Instance as the Execution Mode for your dbt code with Astronomer Cosmos. Schematically, the guide will walk you through the steps required to build the following architecture:

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/cosmos_aci_schematic.png
    :width: 800

Prerequisites
+++++++++++++
1. Docker with docker daemon (Docker Desktop on MacOS). Follow the `Docker installation guide <https://docs.docker.com/engine/install/>`_.
2. Airflow
3. Azure CLI (install guide here: `Azure CLI <https://docs.microsoft.com/en-us/cli/azure/install-azure-cli>`_)
4. Astronomer-cosmos package containing the dbt Azure Container Instance operators
5. Azure account with:
    1. A resource group
    2. A service principal with `Contributor` permissions on the resource group
    3. A Container Registry
    4. A Postgres instance accessible from Azure. (we use an Azure Postgres instance in the example)
6. Docker image built with required dbt project and dbt DAG
7. dbt DAG with dbt Azure Container Instance operators in the Airflow DAGs directory to run in Airflow

More information on how to achieve 2-6 is detailed below.

Note that the steps below will walk you through an example, for which the code can be found HERE

Step-by-step guide
++++++++++++++++++

**Install Airflow and Cosmos**

Create a python virtualenv, activate it, upgrade pip to the latest version and install apache airflow & astronomer-postgres

.. code-block:: bash

    python -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[dbt-postgres,azure-container-instance]"

**Setup Postgres database**

You will need a postgres database running to be used as the database for the dbt project. In order to have it accessible from Azure Container Instance, the easiest way is to create an Azure Postgres instance. For this, run the following (assuming you are logged into your Azure account)

.. code-block:: bash

    az postgres server create -l westeurope -g <<<YOUR_RG>>> -n <<<YOUR_DATABASE_NAME>>> -u dbadmin -p <<<YOUR_PASSWORD_HERE>>> --sku-name B_Gen5_1 --ssl-enforcement Enabled


**Setup Azure Container Registry**
In order to run a container in Azure Container Instance, it needs access to the container image. In our setup, we will use Azure Container Registry for this. To set an Azure Container Registry up, you can use the following bash command:

.. code-block:: bash

    az acr create --name <<<YOUR_REGISTRY_NAME>>> --resource-group <<<YOUR_RG>>> --sku Basic --admin-enabled

**Build the dbt Docker image**

For the Docker operators to work, you need to create a docker image that will be supplied as image parameter to the dbt docker operators used in the DAG.

Clone the `cosmos-example <https://github.com/astronomer/cosmos-example.git>`_ repo

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-example.git
    cd cosmos-example

Create a docker image containing the dbt project files and dbt profile by using the `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/Dockerfile.azure_container_instance>`_, which will be supplied to the Docker operators.

.. code-block:: bash

    docker build -t <<<YOUR_IMAGE_NAME_HERE>>:1.0.0 -f Dockerfile.azure_container_instance .

After this, the image needs to be pushed to the registry of your choice. Note that your image name should contain the name of your registry:
.. code-block:: bash

    docker push <<<YOUR_IMAGE_NAMEHERE>>>:1.0.0

.. note::

    You may need to ensure docker knows to use the right credentials. If using Azure Container Registry, this can be done by running the following command:
    .. code-block:: bash

        az acr login

.. note::

    If running on M1, you may need to set the following envvars for running the docker build command in case it fails

    .. code-block:: bash

        export DOCKER_BUILDKIT=0
        export COMPOSE_DOCKER_CLI_BUILD=0
        export DOCKER_DEFAULT_PLATFORM=linux/amd64

Take a read of the Dockerfile to understand what it does so that you could use it as a reference in your project.

    - The `dbt profile <https://github.com/astronomer/cosmos-example/blob/main/example_postgres_profile.yml>`_ file is added to the image
    - The dags directory containing the `dbt project jaffle_shop <https://github.com/astronomer/cosmos-example/tree/main/dags/dbt/jaffle_shop>`_ is added to the image
    - The dbt_project.yml is replaced with `postgres_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/postgres_profile_dbt_project.yml>`_ which contains the profile key pointing to postgres_profile as profile creation is not handled at the moment for K8s operators like in local mode.

**Setup Airflow Connections**
Now you have the required Azure infrastructure, you still need to add configuration to Airflow to ensure the infrastructure can be used. You'll need 3 connections:

1. ``aci_db``: a Postgres connection to your Azure Postgres instance.
2. ``aci``: an Azure Container Instance connection configured with a Service Principal with sufficient permissions (i.e. ``Contributor`` on the resource group in which you will use Azure Container Instances).
3. ``acr``: an Azure Container Registry connection configured for your Azure Container Registry.

Check out the ``airflow-settings.yml`` file `here <https://github.com/astronomer/cosmos-example/blob/main/airflow_settings.yaml>`_ for an example. If you are using Astro CLI, filling in the right values here will be enough for this to work.

**Setup and Trigger the DAG with Airflow**

Copy the dags directory from cosmos-example repo to your Airflow home

.. code-block:: bash

    cp -r dags $AIRFLOW_HOME/

Run Airflow

.. code-block:: bash

    airflow standalone

.. note::

    You might need to run airflow standalone with ``sudo`` if your Airflow user is not able to access the docker socket URL or pull the images in the Kind cluster.

Log in to Airflow through a web browser ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

Enable and trigger a run of the `jaffle_shop_azure_container_instance <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_azure_container_instance.py>`_ DAG. You will be able to see the following successful DAG run.

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_azure_container_instance.png
    :width: 800
