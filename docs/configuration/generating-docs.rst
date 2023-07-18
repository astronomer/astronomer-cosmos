.. _generating-docs:

Generating Docs
===============

dbt allows you to generate static documentation on your models, tables, and more. You can read more about it in the `official dbt documentation <https://docs.getdbt.com/docs/building-a-dbt-project/documentation>`_. For an example of what the docs look like with the ``jaffle_shop`` project, check out `this site <http://cosmos-docs.s3-website-us-east-1.amazonaws.com/>`_.

Many users choose to generate and serve these docs on a static website. This is a great way to share your data models with your team and other stakeholders.

Cosmos offers two pre-built ways of generating and uploading dbt docs and a fallback option to run custom code after the docs are generated:

- :class:`~cosmos.operators.DbtDocsS3Operator`: generates and uploads docs to a S3 bucket.
- :class:`~cosmos.operators.DbtDocsAzureStorageOperator`: generates and uploads docs to an Azure Blob Storage.
- :class:`~cosmos.operators.DbtDocsOperator`: generates docs and runs a custom callback.

The first two operators require you to have a connection to the target storage. The third operator allows you to run custom code after the docs are generated in order to upload them to a storage of your choice.


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
        conn_id="airflow_db",
        schema="public",
        aws_conn_id="test_aws",
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
        conn_id="airflow_db",
        schema="public",
        azure_conn_id="test_azure",
        container_name="$web",
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
        conn_id="airflow_db",
        schema="public",
        callback=upload_docs,
    )
