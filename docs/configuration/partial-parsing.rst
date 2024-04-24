.. _partial-parsing:

Partial parsing
===============

Starting in the 1.4 version, Cosmos tries to leverage dbt's partial parsing (``partial_parse.msgpack``) to speed up both the task execution and the DAG parsing (if using ``LoadMode.DBT_LS``).

This feature is bound to `dbt partial parsing limitations <https://docs.getdbt.com/reference/parsing#known-limitations>`_.
As an example, ``dbt`` requires the same ``--vars``, ``--target``, ``--profile``, and ``profile.yml`` environment variables (as called by the ``env_var()`` macro) while running dbt commands, otherwise it will reparse the project from scratch.

Profile configuration
---------------------

To respect the dbt requirement of having the same profile to benefit from partial parsing, Cosmos users should either:
* If using Cosmos profile mapping (``ProfileConfig(profile_mapping=...``), disable using mocked profile mappings by setting ``render_config=RenderConfig(enable_mock_profile=False)``
* Declare their own ``profiles.yml`` file, via ``ProfileConfig(profiles_yml_filepath=...)``

If users don't follow these guidelines, Cosmos will use different profiles to parse the dbt project and to run tasks, and the user won't leverage dbt partial parsing.
Their logs will contain multiple ``INFO`` messages similar to the following, meaning that Cosmos are is not using partial parsing:

.. code-block::

    13:33:16  Unable to do partial parsing because profile has changed
    13:33:16  Unable to do partial parsing because env vars used in profiles.yml have changed

dbt vars
--------

If the Airflow scheduler and worker processes run in the same node, users must ensure the dbt ``--vars`` flag is the same in the ``RenderConfig`` and ``ExecutionConfig``.

Otherwise, users may see messages similar to the following in their logs:

.. code-block::

    [2024-03-14, 17:04:57 GMT] {{subprocess.py:94}} INFO - Unable to do partial parsing because config vars, config profile, or config target have changed


Caching
-------

If the dbt project ``target`` directory has a ``partial_parse.msgpack``, Cosmos will attempt to use it.

There is a chance, however, that the file is stale or was generated in a way that is different to how Cosmos runs the dbt commands.

Therefore, Cosmos also caches the most up-to-date ``partial_parse.msgpack`` file after running a dbt command in the `system temporary directory <https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir>`_.
With this, unless there are code changes, each Airflow node should only run the dbt command with a full dbt project parse once, and benefit from partial parsing from then onwards.

It is possible to override the directory that Cosmos uses caching with the Airflow configuration ``[cosmos][cache_dir]`` or environment variable ``AIRFLOW__COSMOS__CACHE_DIR``.

To turn off caching, set the Airflow configuration ``[cosmos][enable_cache]`` or the environment variable ``AIRFLOW__COSMOS__ENABLE_CACHE=0``.

Disabling
---------

To switch off partial parsing in Cosmos, use the argument ``partial_parse=False`` in the ``ProjectConfig``.
