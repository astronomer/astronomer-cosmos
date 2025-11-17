.. _gcp-cloud-run-job:

GCP Cloud Run Job Execution Mode
=======================================
.. versionadded:: 1.7

This tutorial will guide you through the steps required to use Cloud Run Job instance as the Execution Mode for your dbt code with Astronomer Cosmos. This guide will walk you through the steps required to build the following architecture:

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/cosmos_gcp_crj_schematic.png
    :width: 600

Prerequisites
+++++++++++++
1. Docker with docker daemon (Docker Desktop on MacOS). Follow the `Docker installation guide <https://docs.docker.com/engine/install/>`_.
2. Airflow
3. Google Cloud SDK (`install guide <https://cloud.google.com/sdk/docs/install>`_)
4. Astronomer-cosmos package containing the dbt Cloud Run Job operators
5. GCP account with:
    1. A GCP project (`setup guide <https://cloud.google.com/resource-manager/docs/creating-managing-projects#console>`_)
    2. IAM roles:
        * Basic Role: `Owner <https://cloud.google.com/iam/docs/understanding-roles#owner>`_ (control over whole project) or
        * Predefined Roles: `Artifact Registry Administrator <https://cloud.google.com/iam/docs/understanding-roles#artifactregistry.admin>`_, `Cloud Run Developer <https://cloud.google.com/iam/docs/understanding-roles#run.developer>`_ (control over specific services)
    3. Enabled service APIs:
        * Artifact Registry API
        * Cloud Run Admin API
        * BigQuery API
    4. A service account with BigQuery roles: `JobUser <https://cloud.google.com/iam/docs/understanding-roles#bigquery.jobUser>`_ and `DataEditor <https://cloud.google.com/iam/docs/understanding-roles#bigquery.dataEditor>`_
6. Docker image built with required dbt project and dbt DAG
7. dbt DAG with Cloud Run Job operators in the Airflow DAGs directory to run in Airflow

.. note::

    Google Cloud Platform provides free tier on many resources, as well as Free Trial with $300 in credit. Learn more `here <https://cloud.google.com/free/?hl=en>`_.

More information on how to achieve 2-6 is detailed below.


Step-by-step guide
++++++++++++++++++

**Install Airflow and Cosmos**

Create a python virtualenv, activate it, upgrade pip to the latest version and install ``apache airflow`` & ``astronomer cosmos``:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    python3 -m pip install --upgrade pip
    pip install apache-airflow
    pip install "astronomer-cosmos[dbt-bigquery,gcp-cloud-run-job]"

**Setup gcloud and environment variables**

Set environment variables that will be used to create cloud infrastructure. Replace placeholders with your unique GCP ``project id`` and ``region`` of the project:

.. code-block:: bash

    export PROJECT_ID=<<<YOUR_GCP_PROJECT_ID>>>
    export REGION=<<<YOUR_GCP_REGION>>>
    export REPO_NAME="astronomer-cosmos-dbt"
    export IMAGE_NAME="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/cosmos-example"
    export SERVICE_ACCOUNT_NAME="cloud-run-job-sa"
    export DATASET_NAME="astronomer_cosmos_example"
    export CLOUD_RUN_JOB_NAME="astronomer-cosmos-example"

Before we do anything in the GCP project, we first need to authorize gcloud to access the Cloud Platform with Google user credentials:

.. code-block:: bash

    gcloud auth login

You'll receive a link to sign into Google Cloud SDK using a Google Account.

Next, set default ``project id`` using below command:

.. code-block:: bash

    gcloud config set project $PROJECT_ID

In case BigQuery has never been used before in the project, run below command to enable BigQuery API:

.. code-block:: bash

        gcloud services enable bigquery.googleapis.com

**Setup Artifact Registry**

In order to run a container in Cloud Run Job, it needs access to the container image. In our setup, we will use Artifact Registry repository that stores images.
To use Artifact Registry, you need to enable the API first:

.. code-block:: bash

    gcloud services enable artifactregistry.googleapis.com

To set an Artifact Registry repository up, you can use the following bash command:

.. code-block:: bash

    gcloud artifacts repositories create $REPO_NAME \
    --repository-format=docker \
    --location=$REGION \
    --project $PROJECT_ID

**Setup Service Account**

In order to use dbt and make transformations in BigQuery, Cloud Run Job needs some BigQuery permissions. One way to achieve that is to set up a separate ``Service Account`` with needed permissions:

.. code-block:: bash

    # create a service account
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

.. code-block:: bash

    # grant JobUser role
    gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

.. code-block:: bash

    # grant DataEditor role
    gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

**Build the dbt Docker image**

Now, we are going to download an example dbt project and build a Docker image with it.

.. important::

    You need to ensure Docker is using the right credentials to push images. For Artifact Registry, this can be done by running the following command:

    .. code-block:: bash

        gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://$REGION-docker.pkg.dev

    The token will be valid for 1 hour. After that, you need to create another one, if still needed.

Clone the `cosmos-example <https://github.com/astronomer/cosmos-example.git>`_ repo:

.. code-block:: bash

    git clone https://github.com/astronomer/cosmos-example.git
    cd cosmos-example

Open `Dockerfile <https://github.com/astronomer/cosmos-example/blob/main/gcp_cloud_run_job_example/Dockerfile.gcp_cloud_run_job>`_ located in ``gcp_cloud_run_job_example`` folder and change environments variables ``GCP_PROJECT_ID`` and ``GCP_REGION`` to your GCP project id and project region.

Build a Docker image using previously modified ``Dockerfile``, which will be used by Cloud Run Job:

.. code-block:: bash

    docker build -t $IMAGE_NAME -f gcp_cloud_run_job_example/Dockerfile.gcp_cloud_run_job .

.. important::

    Make sure to stay in ``cosmos-example`` directory when running ``docker build`` command.

After this, the image needs to be pushed to the Artifact Registry:

.. code-block:: bash

    docker push $IMAGE_NAME

Take a read of the Dockerfile to understand what it does so that you could use it as a reference in your project.

    - The dags directory containing the `dbt project jaffle_shop <https://github.com/astronomer/cosmos-example/blob/main/dags/dbt/jaffle_shop>`_ is added to the image
    - The `bigquery dbt profile <https://github.com/astronomer/cosmos-example/blob/main/gcp_cloud_run_job_example/example_bigquery_profile.yml>`_ file is added to the image
    - The dbt_project.yml is replaced with `bigquery_profile_dbt_project.yml <https://github.com/astronomer/cosmos-example/blob/main/gcp_cloud_run_job_example/bigquery_profile_dbt_project.yml>`_ which contains the profile key pointing to postgres_profile as profile creation is not handled at the moment for K8s operators like in local mode.

**Create Cloud Run Job instance**

When the image is pushed to Artifact Registry, you can finally create Cloud Run Job with the image and previously created service account.

First, enable Cloud Run Admin API using below command:

.. code-block:: bash

    gcloud services enable run.googleapis.com


Next, set default Cloud Run region to your GCP region:

.. code-block:: bash

    gcloud config set run/region $REGION

Then, run below command to create Cloud Run Job instance:

.. code-block:: bash

    gcloud run jobs create $CLOUD_RUN_JOB_NAME \
    --image=$IMAGE_NAME \
    --task-timeout=180s \
    --max-retries=0 \
    --cpu=1 \
    --memory=512Mi \
    --service-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com

**Setup Airflow Connections**

Now, when you have the required Google Cloud infrastructure, you still need to check Airflow configuration to ensure the infrastructure can be used. You'll need a ``google_cloud_default`` connection in order to work on GCP resources.

Check out an `example <https://github.com/astronomer/cosmos-example/blob/main/airflow_settings.yaml>`_ of the ``airflow-settings.yml`` file. If you are using Astro CLI, filling in the right values here will be enough for this to work.

**Setup and Trigger the DAG with Airflow**

Open `jaffle_shop_gcp_cloud_run_job <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_gcp_cloud_run_job.py>`_ DAG file and update ``GCP_PROJECT_ID`` and ``GCP_LOCATION`` constants with your GCP project id and project region.

When the DAG is configured, copy the ``dags`` directory from ``cosmos-example`` repo to your Airflow home:

.. code-block:: bash

    cp -r dags $AIRFLOW_HOME/

Run Airflow:

.. code-block:: bash

    airflow standalone

.. note::

    You might need to run airflow standalone with ``sudo`` if your Airflow user is not able to access the docker socket URL or pull the images in the Kind cluster.

Log in to Airflow through a web browser ``http://localhost:8080/``, using the user ``airflow`` and the password described in the ``standalone_admin_password.txt`` file.

Enable and trigger a run of the `jaffle_shop_gcp_cloud_run_job <https://github.com/astronomer/cosmos-example/blob/main/dags/jaffle_shop_gcp_cloud_run_job.py>`_ DAG. You will be able to see the following successful DAG run.

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_gcp_cloud_run_job.png
    :width: 800


You can also verify the tables that were created using dbt in BigQuery Studio:

.. figure:: https://github.com/astronomer/astronomer-cosmos/raw/main/docs/_static/jaffle_shop_big_query.png
    :width: 800


**Delete resources**

After the successful tests, don't forget to delete Google Cloud resources to save up costs:

.. code-block:: bash

    # Delete Cloud Run Job instance

    gcloud run jobs delete $CLOUD_RUN_JOB_NAME

.. code-block:: bash

    # Delete BigQuery main and custom dataset specified in dbt schema.yml with all tables included

    bq rm -r -f -d $PROJECT_ID:$DATASET_NAME

    bq rm -r -f -d $PROJECT_ID:dbt_dev

.. code-block:: bash

    # Delete Artifact Registry repository with all images included

    gcloud artifacts repositories delete $REPO_NAME \
    --location=$REGION
