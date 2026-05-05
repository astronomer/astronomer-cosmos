.. _connect_database:

Connect to your database
------------------------

Cosmos supports two methods of authenticating with your database:

- using your own `dbt profiles.yml <../../guides/connect_database/use-your-profiles-yml.html>`_ file
- using `Apache Airflow® <https://airflow.apache.org/>`_ connections via Cosmos' `profile mappings <../../guides/connect_database/use-profile-mapping.html>`_

If you're already interacting with your database from Airflow and have a connection set up, it's recommended
to use a profile mapping to translate that Airflow connection to a dbt profile. This is because it's easier to
maintain a single connection object in Airflow than it is to maintain a connection object in Airflow and a dbt profile
in your dbt project.

If you don't already have an Airflow connection, or if there's no readily-available profile mapping for your database,
you can use your own dbt profiles.yml file.

Regardless of which method you use, you'll need to tell Cosmos which profile and target name it should use. Profile config
is set in the ``cosmos.config.ProfileConfig`` object, like so:

.. code-block:: python

    from cosmos.config import ProfileConfig

    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        # choose one of the following
        profile_mapping=...,
        profiles_yml_filepath=...,
    )

    dag = DbtDag(profile_config=profile_config, ...)
