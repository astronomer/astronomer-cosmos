.. _profile-customise-per-node:

Customising the profile config per dbt node
===========================================

.. versionadded:: 1.9.0


Since Cosmos 1.9.0, it is possible to customise which profile is used per dbt node. This works both when using a
``profile_mapping`` class or when using ``profiles_yml_filepath``.

Let's say the user configures the profile at a ``DbtDag`` or ``DbtTaskGroup`` level as:

.. code-block:: python

    from cosmos.profiles import PostgresUserPasswordProfileMapping

    profile_config = ProfileConfig(
        profile_name="default_profile",
        target_name="default_target",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="default_conn",
            profile_args={"schema": "default_schema"},
        ),
    )

But that for a specific node or group of nodes, the user would like to replace:

* ``profile_name`` to be "non_default_profile" as opposed to "default_profile"
* ``target_name`` to be "stage" as opposed to "default_target"
* ``conn_id`` to be "non_default_connection" as opposed to "default_conn"
* ``schema`` to be "non_default_schema" as opposed to "default_schema"

They could apply this different configuration to all the project seeds by doing:

.. code-block::

    seeds:
      my_dbt_project:
        +meta:
          cosmos:
            profile_config:
              profile_name: non_default_profile
              target_name: stage
              profile_mapping:
                conn_id: non_default_connection
                profile_args:
                  schema: non_default_schema

This same mechanism works per individual dbt nodes, as discussed in :ref:`operator-args-per-node`,
to subsets of nodes selected based on path or other criteria that dbt supports.


Dbt profile config variables
----------------------------
.. versionadded:: 1.4.0

The parts of ``profiles.yml``, which aren't specific to a particular data platform `dbt docs <https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml>`_

.. code-block:: python

    from cosmos.profiles import SnowflakeUserPasswordProfileMapping, DbtProfileConfigVars

    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="my_snowflake_conn_id",
            profile_args={
                "database": "my_snowflake_database",
                "schema": "my_snowflake_schema",
            },
            dbt_config_vars=DbtProfileConfigVars(
                send_anonymous_usage_stats=False,
                partial_parse=True,
                use_experimental_parse=True,
                static_parser=True,
                printer_width=120,
                write_json=True,
                warn_error=True,
                warn_error_options={"include": "all"},
                log_format="text",
                debug=True,
                version_check=True,
            ),
        ),
    )

    dag = DbtDag(profile_config=profile_config, ...)


Disabling dbt event tracking
----------------------------

.. note:
   Deprecated in v.1.4 and will be removed in v2.0.0. Use dbt_config_vars=DbtProfileConfigVars(send_anonymous_usage_stats=False) instead.
.. versionadded:: 1.3

By default `dbt will track events <https://docs.getdbt.com/reference/global-configs/usage-stats>`_ by sending anonymous usage data
when dbt commands are invoked. Users have an option to opt out of event tracking by updating their ``profiles.yml`` file.

If you'd like to disable this behavior in the Cosmos generated profile, you can pass ``disable_event_tracking=True`` to the profile mapping like in
the example below:

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
            disable_event_tracking=True,
        ),
    )

    dag = DbtDag(profile_config=profile_config, ...)
