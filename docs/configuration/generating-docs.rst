.. _generating-docs:

Generating Docs
===============

dbt allows you to generate static documentation on your models, tables, and more. You can read more about it in the `official dbt documentation <https://docs.getdbt.com/docs/building-a-dbt-project/documentation>`_. For an example of what the docs look like with the ``jaffle_shop`` project, check out `this site <http://cosmos-demo-dbt-docs.s3-website.eu-north-1.amazonaws.com/>`_.

After generating the dbt docs, you can host them natively within Airflow via the Cosmos Airflow plugin; see `Hosting Docs <hosting-docs.html>`__ for more information.

Alternatively, many users choose to serve these docs on a separate static website. This is a great way to share your data models with a broad array of stakeholders.

.. note::
    The CosmosPlugin is not available for Airflow 3 yet as the compatibility is still being worked on. Hence, the dbt docs cannot be hosted and used in Airflow 3 yet.


Cosmos offers two pre-built ways of generating and uploading dbt docs and a fallback option to run custom code after the docs are generated:

- :class:`~cosmos.operators.DbtDocsS3Operator`: generates and uploads docs to a S3 bucket.
- :class:`~cosmos.operators.DbtDocsAzureStorageOperator`: generates and uploads docs to an Azure Blob Storage.
- :class:`~cosmos.operators.DbtDocsGCSOperator`: generates and uploads docs to a GCS bucket.
- :class:`~cosmos.operators.DbtDocsOperator`: generates docs and runs a custom callback.

The first three operators require you to have a connection to the target storage. The last operator allows you to run custom code after the docs are generated in order to upload them to a storage of your choice.


Examples
----------------------

Upload to S3
~~~~~~~~~~~~~~~~~~~~~~~

S3 supports serving static files directly from a bucket. To learn more (and to set it up), check out the `official S3 documentation <https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html>`_.

You can use the :class:`~cosmos.operators.DbtDocsS3Operator` to generate and upload docs to a S3 bucket. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    from cosmos.operators import DbtDocsS3Operator

    # then, in your DAG code:
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir="path/to/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        connection_id="test_aws",
        bucket_name="test_bucket",
    )

Upload to Azure Blob Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Azure Blob Storage supports serving static files directly from a container. To learn more (and to set it up), check out the `official documentation <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-static-website>`_.

You can use the :class:`~cosmos.operators.DbtDocsAzureStorageOperator` to generate and upload docs to an Azure Blob Storage. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    from cosmos.operators import DbtDocsAzureStorageOperator

    # then, in your DAG code:
    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir="path/to/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        connection_id="test_azure",
        bucket_name="$web",
    )

Upload to GCS
~~~~~~~~~~~~~~~~~~~~~~~

GCS supports serving static files directly from a bucket. To learn more (and to set it up), check out the `official GCS documentation <https://cloud.google.com/appengine/docs/standard/serving-static-files?tab=python>`_.

You can use the :class:`~cosmos.operators.DbtDocsGCSOperator` to generate and upload docs to a GCS bucket. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    from cosmos.operators import DbtDocsGCSOperator

    # then, in your DAG code:
    generate_dbt_docs_gcs = DbtDocsGCSOperator(
        task_id="generate_dbt_docs_gcs",
        project_dir="path/to/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        connection_id="test_gcs",
        bucket_name="test_bucket",
    )

Choosing a folder
~~~~~~~~~~~~~~~~~~~~~~~

All the DbtDocsOperators support specification of a custom folder (prefix) to place documentation in on the target cloud storage. This can be done by
adding a ``folder_dir`` parameter to the operator definition.

Static Flag
~~~~~~~~~~~~~~~~~~~~~~~

All of the DbtDocsOperator accept the ``--static`` flag. To learn more about the static flag, check out the `original PR on dbt-core <https://github.com/dbt-labs/dbt-docs/pull/465>`_.
The static flag is used to generate a single doc file that can be hosted directly from cloud storage.
By having a single documentation file, you can make use of Access control can be configured through Identity-Aware Proxy (IAP), and making it easy to host.

.. note::
    The static flag is only available from dbt-core >=1.7

The following code snippet shows how to provide this flag with the default jaffle_shop project:


.. code-block:: python

    from cosmos.operators import DbtDocsGCSOperator

    # then, in your DAG code:
    generate_dbt_docs_gcs = DbtDocsGCSOperator(
        task_id="generate_dbt_docs_gcs",
        project_dir="path/to/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        connection_id="test_gcs",
        bucket_name="test_bucket",
        dbt_cmd_flags=["--static"],
    )

Custom Callback
~~~~~~~~~~~~~~~~~~~~~~~

If you want to run custom code after the docs are generated, you can use the :class:`~cosmos.operators.DbtDocsOperator`. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    from cosmos.operators import DbtDocsOperator

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook


    def upload_to_s3(project_dir: str):
        # Upload the docs to S3
        hook = S3Hook(aws_conn_id="aws_conn_id")

        for dir, _, files in os.walk(project_dir):
            for file in files:
                hook.load_file(
                    filename=os.path.join(dir, file),
                    key=file,
                    bucket_name="my-bucket",
                    replace=True,
                )


    def upload_docs(project_dir):
        # upload docs to a storage of your choice
        # you only need to upload the following files:
        # - f"{project_dir}/target/index.html"
        # - f"{project_dir}/target/manifest.json"
        # - f"{project_dir}/target/graph.gpickle"
        # - f"{project_dir}/target/catalog.json"
        pass


    # then, in your DAG code:
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="path/to/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        callback=upload_docs,
    )
