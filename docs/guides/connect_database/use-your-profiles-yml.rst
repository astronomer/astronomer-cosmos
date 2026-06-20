.. _use-your-profiles-yml:

Use your own profiles.yml file
==============================

If you don't want to use `Apache Airflow® <https://airflow.apache.org/>`_ connections, or if there's no readily-available profile mapping for your database,
you can use your own dbt profiles.yml file. To do so, you'll need to pass the path to your profiles.yml file to the
``profiles_yml_filepath`` argument in ``ProfileConfig``.

For example, the code snippet below points Cosmos at a ``profiles.yml`` file and instructs Cosmos to use the
``my_snowflake_profile`` profile and ``dev`` target:

.. code-block:: python

    from cosmos.config import ProfileConfig

    profile_config = ProfileConfig(
        profile_name="my_snowflake_profile",
        target_name="dev",
        profiles_yml_filepath="/path/to/profiles.yml",
    )

    dag = DbtDag(profile_config=profile_config, ...)


Example: AWS Glue (dbt-glue)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

AWS Glue is not natively supported by Cosmos' built-in profile mappings, but is fully supported when you provide your own ``profiles.yml`` file.

First, configure your ``profiles.yml`` file (for example, at ``/path/to/profiles.yml``):

.. code-block:: yaml

    my_glue_profile:
      target: dev
      outputs:
        dev:
          type: glue
          role_arn: arn:aws:iam::123456789012:role/GlueExecutionRoleExample
          region: us-east-1
          schema: my_schema
          database: my_database

Then, point Cosmos to your ``profiles.yml`` file using ``ProfileConfig``:

.. code-block:: python

    from cosmos.config import ProfileConfig

    profile_config = ProfileConfig(
        profile_name="my_glue_profile",
        target_name="dev",
        profiles_yml_filepath="/path/to/profiles.yml",
    )

    dag = DbtDag(profile_config=profile_config, ...)
