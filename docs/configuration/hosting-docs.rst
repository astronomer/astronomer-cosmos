.. hosting-docs:

Hosting Docs
============

dbt docs can be served directly from the `Apache Airflow® <https://airflow.apache.org/>`_ webserver with the Cosmos Airflow plugin, without requiring the user to set up anything outside of Airflow. This page describes how to host docs in the Airflow webserver directly, although some users may opt to host docs externally.

Overview
~~~~~~~~

The dbt docs are available in the Airflow menu under ``Browse > dbt docs``:

.. image:: /_static/location_of_dbt_docs_in_airflow.png
    :alt: Airflow UI - Location of dbt docs in menu
    :align: center

In order to access the dbt docs, you must specify the following config variables:

- ``cosmos.dbt_docs_dir``: A path to where the docs are being hosted.
- (Optional) ``cosmos.dbt_docs_conn_id``: A conn ID to use for a cloud storage deployment. If not specified _and_ the URI points to a cloud storage platform, then the default conn ID for the AWS/Azure/GCP hook will be used.

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = path/to/docs/here
    dbt_docs_conn_id = my_conn_id

or as an environment variable:

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="path/to/docs/here"
    AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="my_conn_id"

The path can be either a folder in the local file system the webserver is running on, or a URI to a cloud storage platform (S3, GCS, Azure).

If your docs were generated using the ``--static`` flag, you can set the index filename using ``dbt_docs_index_file_name``:

.. code-block:: cfg

    [cosmos]
    dbt_docs_index_file_name = static_index.html


Host from Cloud Storage
~~~~~~~~~~~~~~~~~~~~~~~

For typical users, the recommended setup for hosting dbt docs would look like this:

1. Generate the docs via one of Cosmos' pre-built operators for generating dbt docs (see `Generating Docs <generating-docs.html>`__ for more information)
2. Wherever you dumped the docs, set your ``cosmos.dbt_docs_dir`` to that location.
3. If you want to use a conn ID other than the default connection, set your ``cosmos.dbt_docs_conn_id``. Otherwise, leave this blank.

AWS S3 Example
^^^^^^^^^^^^^^

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = s3://my-bucket/path/to/docs
    dbt_docs_conn_id = aws_default

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="s3://my-bucket/path/to/docs"
    AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="aws_default"

Google Cloud Storage Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = gs://my-bucket/path/to/docs
    dbt_docs_conn_id = google_cloud_default

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="gs://my-bucket/path/to/docs"
    AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="google_cloud_default"

Azure Blob Storage Example
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = wasb://my-container/path/to/docs
    dbt_docs_conn_id = wasb_default

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="wasb://my-container/path/to/docs"
    AIRFLOW__COSMOS__DBT_DOCS_CONN_ID="wasb_default"

Host from Local Storage
~~~~~~~~~~~~~~~~~~~~~~~

By default, Cosmos will not generate docs on the fly. Local storage only works if you are pre-compiling your dbt project before deployment.

If your Airflow deployment process involves running ``dbt compile``, you will also want to add ``dbt docs generate`` to your deployment process as well to generate all the artifacts necessary to run the dbt docs from local storage.

By default, dbt docs are generated in the ``target`` folder; so that will also be your docs folder by default.

For example, if your dbt project directory is ``/usr/local/airflow/dags/my_dbt_project``, then by default your dbt docs directory will be ``/usr/local/airflow/dags/my_dbt_project/target``:

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = /usr/local/airflow/dags/my_dbt_project/target

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="/usr/local/airflow/dags/my_dbt_project/target"

Using docs out of local storage has a couple downsides. First, some values in the dbt docs can become stale, unless the docs are periodically refreshed and redeployed:

- Counts of the numbers of rows.
- The compiled SQL for incremental models before and after the first run.

Second, deployment from local storage may only be partially compatible with some managed Airflow systems.
Compatibility will depend on the managed Airflow system, as each one works differently.

For example, Astronomer does not update the resources available to the webserver instance when ``--dags`` is specified during deployment, meaning that the dbt dcs will not be updated when this flag is used.

.. note::
    Managed Airflow on Astronomer Cloud does not provide the webserver access to the DAGs folder.
    If you want to host your docs in local storage with Astro, you should host them in a directory other than ``dags/``.
    For example, you can set your ``AIRFLOW__COSMOS__DBT_DOCS_DIR`` to ``/usr/local/airflow/dbt_docs_dir`` with the following pre-deployment script:

    .. code-block:: bash

        dbt docs generate
        mkdir dbt_docs_dir
        cp dags/dbt/target/manifest.json dbt_docs_dir/manifest.json
        cp dags/dbt/target/catalog.json dbt_docs_dir/catalog.json
        cp dags/dbt/target/index.html dbt_docs_dir/index.html

Host from HTTP/HTTPS
~~~~~~~~~~~~~~~~~~~~

.. code-block:: cfg

    [cosmos]
    dbt_docs_dir = https://my-site.com/path/to/docs

.. code-block:: shell

    AIRFLOW__COSMOS__DBT_DOCS_DIR="https://my-site.com/path/to/docs"


You do not need to set a ``dbt_docs_conn_id`` when using HTTP/HTTPS.
If you do set the ``dbt_docs_conn_id``, then the ``HttpHook`` will be used.
