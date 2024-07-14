.. _caching:

Caching
=======

This page explains the caching strategies in ``astronomer-cosmos`` Astronomer Cosmos behavior.

All Cosmos caching mechanisms can be enabled or turned off in the ``airflow.cfg`` file or using environment variables.

.. note::
    For more information, see `configuring a Cosmos project <./project-config.html>`_.

Depending on the Cosmos version, it creates a cache for two types of data:

- The ``dbt ls`` output
- The dbt ``partial_parse.msgpack`` file

It is possible to turn off any cache in Cosmos by exporting the environment variable ``AIRFLOW__COSMOS__ENABLE_CACHE=0``.
Disabling individual types of cache in Cosmos is also possible, as explained below.

Caching the dbt ls output
~~~~~~~~~~~~~

(Introduced in Cosmos 1.5)

While parsing a dbt project using `LoadMode.DBT_LS <./parsing-methods.html#dbt-ls>`_, Cosmos uses subprocess to run ``dbt ls``.
This operation can be very costly; it can increase the DAG parsing times and affect not only the scheduler DAG processing but
also the tasks queueing time.

Cosmos 1.5 introduced a feature to mitigate the performance issue associated with ``LoadMode.DBT_LS`` by caching the output
of this command as an  `Airflow Variable <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html>`_.
Based on an initial `analysis <https://github.com/astronomer/astronomer-cosmos/pull/1014>`_, enabling this setting reduced some DAGs ask queueing from 30s to 0s. Additionally, some users `reported improvements of 84% <https://github.com/astronomer/astronomer-cosmos/pull/1014#issuecomment-2168185343>`_ in the DAG run time.

This feature is on by default. To turn it off, export the following environment variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=0``.

**How the cache is refreshed**

Users can purge or delete the cache via Airflow UI by identifying and deleting the cache key.

Cosmos will refresh the cache in a few circumstances:

* if any files of the dbt project change
* if one of the arguments that affect the dbt ls command execution changes

To evaluate if the dbt project changed, it calculates the changes using a few of the MD5 of all the files in the directory.

Additionally, if any of the following DAG configurations are changed, we'll automatically purge the cache of the DAGs that use that specific configuration:

* ``ProjectConfig.dbt_vars``
* ``ProjectConfig.env_vars``
* ``ProjectConfig.partial_parse``
* ``RenderConfig.env_vars``
* ``RenderConfig.exclude``
* ``RenderConfig.select``
* ``RenderConfig.selector``

Finally, if users would like to define specific Airflow variables that, if changed, will cause the recreation of the cache, they can specify those by using:

* ``RenderConfig.airflow_vars_to_purge_cache``

Example:

.. code-block:: python

    RenderConfig(airflow_vars_to_purge_cache == ["refresh_cache"])

**Cleaning up stale cache**

Not rarely, Cosmos DbtDags and DbtTaskGroups may be renamed or deleted. In those cases, to clean up the Airflow metadata database, it is possible to use the method ``delete_unused_dbt_ls_cache``.

The method deletes the Cosmos cache stored in Airflow Variables based on the last execution of their associated DAGs.

As an example, the following clean-up DAG will delete any cache associated with Cosmos that has not been used for the last five days:

.. literalinclude:: ../../dev/dags/example_cosmos_cleanup_dag.py
    :language: python
    :start-after: [START cache_example]
    :end-before: [END cache_example]

**Cache key**

The Airflow variables that represent the dbt ls cache are prefixed by ``cosmos_cache``.
When using ``DbtDag``, the keys use the DAG name. When using ``DbtTaskGroup``, they contain the ``TaskGroup`` and parent task groups and DAG.

Examples:

* The ``DbtDag`` "cosmos_dag" will have the cache represented by "cosmos_cache__basic_cosmos_dag".
* The ``DbtTaskGroup`` "customers" declared inside the DAG "basic_cosmos_task_group" will have the cache key "cosmos_cache__basic_cosmos_task_group__customers".

**Cache value**

The cache values contain a few properties:

* ``last_modified`` timestamp, represented using the ISO 8601 format.
* ``version`` is a hash that represents the version of the dbt project and arguments used to run dbt ls by the time Cosmos created the cache
* ``dbt_ls_compressed`` represents the dbt ls output compressed using zlib and encoded to base64 so Cosmos can record the value as a compressed string in the Airflow metadata database.
* ``dag_id`` is the DAG associated to this cache
* ``task_group_id`` is the TaskGroup associated to this cache
* ``cosmos_type`` is either ``DbtDag`` or ``DbtTaskGroup``


Caching the partial parse file
~~~~~~~~~~~~~

(Introduced in Cosmos 1.4)

After parsing the dbt project, dbt stores an internal project manifest in a file called ``partial_parse.msgpack`` (`official docs <https://docs.getdbt.com/reference/parsing#partial-parsing>`_).
This file contributes significantly to the performance of running dbt commands when the dbt project did not change.

Cosmos 1.4 introduced `support to partial parse files <https://github.com/astronomer/astronomer-cosmos/pull/800>`_ both
provided by the user, and also by storing in the disk temporary folder in the Airflow scheduler and worker node the file
generated after running dbt commands.

Users can customize where to store the cache using the setting ``AIRFLOW__COSMOS__CACHE_DIR``.

It is possible to switch off this feature by exporting the environment variable ``AIRFLOW__COSMOS__ENABLE_CACHE_PARTIAL_PARSE=0``.

For more information, read the `Cosmos partial parsing documentation <./partial-parsing.html>`_


Caching the profiles
~~~~~~~~~~~~~~~~~~~~~~~~

(Introduced in Cosmos 1.5)

Cosmos 1.5 introduced `support to profile caching <https://github.com/astronomer/astronomer-cosmos/pull/1046>`_,
enabling caching for the profile mapping in the path specified by env ``AIRFLOW__COSMOS__CACHE_DIR`` and ``AIRFLOW__COSMOS__PROFILE_CACHE_DIR_NAME``.
This feature facilitates the reuse of Airflow connections and ``profiles.yml``.

Users have the flexibility to customize the cache storage location using the settings ``AIRFLOW__COSMOS__CACHE_DIR`` and ``AIRFLOW__COSMOS__PROFILE_CACHE_DIR_NAME``.

To disable this feature, users can set the environment variable ``AIRFLOW__COSMOS__ENABLE_CACHE_PROFILE=False``
