Generating Docs
================

Cosmos offers a way to generate dbt docs (i.e. ``dbt docs generate``) via the :class:`~cosmos.providers.dbt.core.operators.DbtDocsOperator` class. This operator is a wrapper around the dbt CLI command ``dbt docs generate`` and exposes a ``callback`` parameter that allows you to run custom code after the docs are generated.

The ``callback`` parameter is a function that takes in a single argument, ``project_dir``. You can use the ``callback`` to decide what to do with the generated docs. For example, you can upload the docs to a cloud storage bucket or commit them to a git repo.

Examples
----------------------

Upload to S3
~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``callback`` parameter to upload the generated docs to an S3 bucket. The following code snippet shows how to do this with the default jaffle_shop project:

.. code-block:: python

    import os

    from cosmos.providers.dbt.core.operators import DbtDocsOperator
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    def upload_to_s3(project_dir: str):
        # Upload the docs to S3
        hook = S3Hook(aws_conn_id='aws_conn_id')

        for dir, _, files in os.walk(project_dir):
            for file in files:
                hook.load_file(
                    filename=os.path.join(dir, file),
                    key=file,
                    bucket_name='my-bucket',
                    replace=True,
                )


    # then, in your DAG code:
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/usr/local/airflow/dags/dbt/jaffle_shop",
        schema="public",
        conn_id="airflow_db",
        callback=upload_to_s3,
    )
