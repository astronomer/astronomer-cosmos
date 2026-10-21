.. _use-your-profiles-yml:

Use your own profiles.yml file
==============================

If you don't want to use `Apache Airflow® <https://airflow.apache.org/>`_ connections, or if there's no readily-available profile mapping for your database,
you can use your own dbt profiles.yml file. To do so, you'll need to pass the path to your profiles.yml file to the
``profiles_yml_filepath`` argument in ``ProfileConfig``.

.. tip::
   This approach works with any adapter compatible with **dbt Core** or **dbt Fusion**, including both official `Trusted Adapters <https://docs.getdbt.com/docs/trusted-adapters>`_ and `Community Adapters <https://docs.getdbt.com/docs/community-adapters>`_.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`AWS Glue <https://docs.getdbt.com/docs/local/connect-data-platform/glue-setup?version=2.0&name=Fusion>`_ is not natively supported by Cosmos' built-in profile mappings, but is fully supported when you provide your own ``profiles.yml`` file.

First, configure your ``profiles.yml`` file (for example, at ``/path/to/profiles.yml``):

.. code-block:: yaml

    my_glue_profile:
      target: dev
      outputs:
        dev:
          type: glue
          role_arn: arn:aws:iam::1234567890:role/GlueExecutionRoleExample
          region: us-east-1
          workers: 2
          worker_type: G.1X
          idle_timeout: 10
          schema: "dbt_demo"
          session_provisioning_timeout_in_seconds: 120
          location: "s3://dbt_demo_bucket/dbt_demo_data"

Then, point Cosmos to your ``profiles.yml`` file using ``ProfileConfig``:

.. code-block:: python

    from cosmos.config import ProfileConfig

    profile_config = ProfileConfig(
        profile_name="my_glue_profile",
        target_name="dev",
        profiles_yml_filepath="/path/to/profiles.yml",
    )

    dag = DbtDag(profile_config=profile_config, ...)
