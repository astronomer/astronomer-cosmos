.. _use-your-profiles-yml:

Use your own profiles.yml file
------------------------------

If you don't want to use Airflow connections, or if there's no readily-available profile mapping for your database,
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
