Generating Docs
================

Cosmos offers a way to generate dbt docs (i.e. ``dbt docs generate``) via the :class:`~cosmos.providers.dbt.core.operators.DbtDocsOperator` class. This operator is a wrapper around the dbt CLI command ``dbt docs generate`` and exposes a ``callback`` parameter that allows you to run custom code after the docs are generated.

The ``callback`` parameter is a function that takes in a single argument, ``tmp_project_dir``.
Currently we support S3 and Azure Blob storage uploading from ``DbtDocsS3Operator`` / ``DbtDocsAzureStorageOperator``. These operators handle the appropriate callback invocation automatically in the background.
Moreover you can use the ``DbtDocsOperator`` and custom ``callback`` to decide what to do with the generated docs. For example, you can commit them to a git repo.

Examples
----------------------

Upload to S3
~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``DbtDocsS3Operator`` to generate and upload docs to a S3 bucket. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    import os

    from cosmos.providers.dbt.core.operators import DbtDocsS3Operator

    # then, in your DAG code:
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir=DBT_ROOT_PATH,
        conn_id="airflow_db",
        schema='public',
        target_conn_id="test_aws",
        bucket_name="test_bucket",
    )

Upload to Azure Blob Storage
~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``DbtDocsAzureStorageOperator`` to generate and upload docs to an Azure Blob Storage. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    import os

    from cosmos.providers.dbt.core.operators import DbtDocsAzureStorageOperator

    # then, in your DAG code:
    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir=DBT_ROOT_PATH,
        conn_id="airflow_db",
        schema='public',
        target_conn_id="test_azure",
        container_name="$web",
    )
