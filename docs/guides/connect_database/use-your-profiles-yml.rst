.. _use-your-profiles-yml:

Use your own profiles.yml file
==============================

If you don't want to use `Apache Airflow® <https://airflow.apache.org/>`_ connections, or if there's no readily-available profile mapping for your database,
you can use your own dbt profiles.yml file. To do so, you'll need to pass the path to your profiles.yml file to the
``profiles_yml_filepath`` argument in ``ProfileConfig``.

.. note::

   This method supports **any dbt adapter**, including those without a built-in ``ProfileMapping``
   in Cosmos (e.g. `dbt-glue <https://github.com/aws-samples/dbt-glue>`_). If your database is not
   listed in the `available profile mappings <../use-profile-mapping.html>`_, using your own
   ``profiles.yml`` is the recommended approach and retains all Cosmos features (task-group
   rendering, OpenLineage lineage, etc.).

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

Using dbt-glue with a custom profiles.yml
-----------------------------------------

If you are using `dbt-glue <https://github.com/aws-samples/dbt-glue>`_ (or any other adapter without a built-in
profile mapping), provide the adapter-specific ``profiles.yml`` via ``profiles_yml_filepath``:

.. code-block:: python

    from cosmos.config import ProfileConfig

    profile_config = ProfileConfig(
        profile_name="glue_profile",
        target_name="dev",
        profiles_yml_filepath="/path/to/glue/profiles.yml",
    )

    dag = DbtDag(profile_config=profile_config, ...)

Your ``profiles.yml`` should follow the `dbt-glue profile configuration <https://github.com/aws-samples/dbt-glue#configuration>`_
as documented by the adapter maintainer.
