.. hosting-docs:

Hosting Docs
============

dbt docs can be served directly from the `Apache Airflow® <https://airflow.apache.org/>`_ webserver(Airflow 2) or API server(Airflow 3) with the Cosmos Airflow plugin, without requiring the user to set up anything outside of Airflow. This page describes how to host docs in the Airflow webserver/API server directly, although some users may opt to host docs externally.


Airflow 2 and Airflow 3 use different UI plugin systems.

- Airflow 2: Cosmos uses a FAB/Flask view under ``Browse > dbt Docs``. Supports only a single project.
- Airflow 3: Cosmos registers a FastAPI external view for each configured dbt project and a sub-application under ``/cosmos``.


Overview
~~~~~~~~


Airflow 3
~~~~~~~~~

The dbt docs are available in the Airflow menu under ``Browse``:

.. image:: /_static/location_of_dbt_docs_in_airflow3.png
    :alt: Airflow UI - Location of dbt docs in menu in Airflow 3
    :align: center

For Airflow 3, Cosmos exposes the docs under a FastAPI sub-app and external view entries under **Browse**.
You can configure one or more projects via ``[cosmos].dbt_docs_projects``:

.. code-block:: ini

   [cosmos]
   dbt_docs_projects = {
     "core": {"dir": "/path/to/core/target", "index": "index.html", "name": "dbt Docs (Core)"},
     "mart": {"dir": "s3://bucket/path/to/mart/target", "conn_id": "aws_default", "name": "dbt Docs (Mart)"}
   }

Or using environment variables (recommended for containers):

.. code-block:: bash

   export AIRFLOW__COSMOS__DBT_DOCS_PROJECTS='{"core":{"dir":"/path/to/core/target","index":"index.html","name":"dbt Docs (Core)"},"mart":{"dir":"s3://bucket/path/to/mart/target","conn_id":"aws_default","name":"dbt Docs (Mart)"}}'

Endpoints per project (``<slug>``):

- ``/cosmos/<slug>/dbt_docs`` (wrapper page)
- ``/cosmos/<slug>/dbt_docs_index.html`` (docs index)
- ``/cosmos/<slug>/manifest.json`` and ``/cosmos/<slug>/catalog.json``


Local vs Remote docs (Airflow 3)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Local docs (filesystem paths)
''''''''''''''''''''''''''''''

- When a project's ``dir`` is a local path (e.g. ``/usr/local/airflow/dbt/jaffle_shop/target``):
  - The plugin serves ``/cosmos/<slug>/dbt_docs_index.html`` by reading ``<dir>/<index>`` (default ``index.html``).
  - Example:

    .. code-block:: ini

       [cosmos]
       dbt_docs_projects = {
         "core": {"dir": "/usr/local/airflow/dbt/jaffle_shop/target", "index": "index.html"}
       }

Remote docs (S3, GCS, Azure, HTTP[S])
''''''''''''''''''''''''''''''''''''''

- When ``dir`` is a remote URI (e.g. ``s3://...``, ``gs://...``, ``abfs://...``, ``wasb://...``, ``http(s)://...``):
  - Files (index, manifest.json, catalog.json) are read via Airflow ObjectStorage (uPath/fsspec).
  - For cloud object storage, set ``conn_id`` or embed it inline (e.g. ``s3://aws_default@my-bucket/path``).
  - HTTP/HTTPS does not require a connection (an Airflow HTTP connection can be used if desired).
  - Example (S3):

    .. code-block:: ini

       [cosmos]
       dbt_docs_projects = {
         "mart": {"dir": "s3://my-bucket/dbt/mart/target", "conn_id": "aws_default"}
       }

Notes and behavior
''''''''''''''''''

- Missing artifacts return HTTP 404 with the attempted path; other IO errors return HTTP 500 and are logged.
- The docs iframe page (``/cosmos/<slug>/dbt_docs``) links to the index and shows within the Airflow UI; the index adds a small inline script to keep browser back/forward behavior intuitive.
- If you deploy behind a path prefix (e.g. Astronomer hosted Airflow deployments), menu links include the prefix automatically using ``AIRFLOW__API__BASE_URL``.

Airflow 2
~~~~~~~~~

.. important::
   The remainder of this page applies to Airflow 2 (FAB/Flask) plugins only.

The dbt docs are available in the Airflow menu under ``Browse > dbt docs``:

.. image:: /_static/location_of_dbt_docs_in_airflow2.png
    :alt: Airflow UI - Location of dbt docs in menu
    :align: center

In order to access the dbt docs in Airflow 2, you must specify the following config variables:

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
