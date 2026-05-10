.. _use-profile-mapping:

Use a profile mapping
---------------------

Profile mappings are utilities provided by Cosmos that translate `Apache Airflow® <https://airflow.apache.org/>`_ connections to dbt profiles. This means that
you can use the same connection objects you use in Airflow to authenticate with your database in dbt. To do so, there's
a class in Cosmos for each Airflow connection to dbt profile mapping.

You can find the available profile mappings on the left-hand side of this page. Each profile mapping is imported from
``cosmos.profiles`` and takes two arguments:

- ``conn_id``: the Airflow connection ID to use.
- ``profile_args``: a dictionary of additional arguments to pass to the dbt profile. This is useful for specifying
  values that are not in the Airflow connection. This also acts as an override for any values that are in the Airflow
  connection but should be overridden.

Below is an example of using the Snowflake profile mapping, where we take most arguments from the Airflow connection
but override the ``database`` and ``schema`` values:

.. code-block:: python

    from cosmos.profiles import SnowflakeUserPasswordProfileMapping

    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="my_snowflake_conn_id",
            profile_args={
                "database": "my_snowflake_database",
                "schema": "my_snowflake_schema",
            },
        ),
    )

    dag = DbtDag(profile_config=profile_config, ...)

Note that when using a profile mapping, the profiles.yml file gets generated with the profile name and target name
you specify in ``ProfileConfig``.
