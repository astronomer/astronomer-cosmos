.. _caching:

Caching
=======

This page explains the caching strategies available in ``astronomer-cosmos`` Astronomer Cosmos behavior.
They can be enabled or disabled in the ``airflow.cfg file`` or using environment variables.

.. note::
    For more information, see `configuring a Cosmos project <./project-config.html>`_.

Depending on the Cosmos version, it creates cache for two types of data:

- The ``dbt ls`` output
- The dbt ``partial_parse.msgpack`` file

It is possible to disable any type of cache in Cosmos by exporting the environment variable ``AIRFLOW__COSMOS__ENABLE_CACHE=0``.
It is also possible to disable individual types of cache in Cosmos, as explained below.

Caching the dbt ls output
~~~~~~~~~~~~~

(Introduced in Cosmos 1.5)

While parsing a dbt project using `LoadMode.DBT_LS <./parsing-methods.html#dbt-ls>`_, Cosmos uses subprocess to run ``dbt ls``.
This operation can be very costly, increasing the DAG parsing times and affecting not only the scheduler DAG processing but
the tasks queueing time.

Cosmos 1.5 introduced a feature to mitigate the performance issue associated to ``LoadMode.DBT_LS`` by caching the output
of this command as an  `Airflow Variable <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html>`_.
Based on an initial analysis `Airflow Variable <https://github.com/astronomer/astronomer-cosmos/pull/1014>`_, the task queueing
times can reduce from 30s to 0s. Some users `reported improvements of 84% <https://github.com/astronomer/astronomer-cosmos/pull/1014#issuecomment-2168185343>`_ in the DAG run time.

This feature is enabled by default and can be disabled by setting ``AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=0``.

**How the cache is refreshed**

Users can purge or delete the cache via Airflow UI by identifying and deleting the cache key.

The cache is automatically refreshed in case any files of the dbt project change. Changes are calculated using the MD5 of all the files in the directory.

Additionally, if any of the following DAG configurations are changed, we'll automatically purge the cache of the DAGs that use that specific configuration:

* ``ProjectConfig.dbt_vars``
* ``ProjectConfig.env_vars``
* ``ProjectConfig.partial_parse``
* ``RenderConfig.env_vars``
* ``RenderConfig.exclude``
* ``RenderConfig.select``
* ``RenderConfig.selector``

The following argument was introduced in case users would like to define Airflow variables that could be used to refresh the cache (it expects a list with Airflow variable names):

* ``RenderConfig.airflow_vars_to_purge_cache``

Example:

.. code-block:: python

    RenderConfig(airflow_vars_to_purge_cache == ["refresh_cache"])


If the same variable is used to purge the cache of multiple DAGs, this may lead to many cache misses and Airflow scheduler
having to re-run ``dbt ls`` for many DAGs.

**Cache key**

The Airflow variables that represent the dbt ls cache are prefixed by ``cosmos_cache``.
When using ``DbtDag``, the keys use the DAG name. When using ``DbtTaskGroup``, they contain the ``TaskGroup`` and parent task groups and DAG.

Examples:

* The ``DbtDag`` "cosmos_dag" will have the cache represented by "cosmos_cache__basic_cosmos_dag".
* The ``DbtTaskGroup`` "customers" declared inside the DAG "basic_cosmos_task_group" will have the cache key "cosmos_cache__basic_cosmos_task_group__customers".

**Cache value**

The cache values contain a few properties:

* ``last_modified`` timestamp, represented using the ISO 8601 format.
* ``version`` is a hash that represents the version of the dbt project and arguments used to run dbt ls by the time the cache was created
* ``dbt_ls_compressed`` represents the dbt ls output compressed using zlib and encoded to base64 to be recorded as a string to the Airflow metadata database.
* ``dag_id`` is the DAG associated to this cache
* ``task_group_id`` is the TaskGroup associated to this cache
* ``cosmos_type`` is either ``DbtDag`` or ``DbtTaskGroup``


Caching the partial parse file
~~~~~~~~~~~~~

(Introduced in Cosmos 1.4)

After parsing the dbt project, dbt stores an internal project manifest in a file called ``partial_parse.msgpack`` (`official docs <https://docs.getdbt.com/reference/parsing#partial-parsing>`_).
This file contributes significantly with the performance of running dbt commands when the dbt project did not change.

Cosmos 1.4 introduced `support to partial parse files <https://github.com/astronomer/astronomer-cosmos/pull/800>`_ both
provided by the user, and also by storing into the disk temporary folder in the Airflow scheduler and worker node the file
generated after running dbt commands.

For more information, check the `Cosmos partial parsing documentation <./partial-parsing.html>`_
