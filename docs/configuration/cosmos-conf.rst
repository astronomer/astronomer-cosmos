Cosmos Config
=============

This page lists all available Airflow configurations that affect ``astronomer-cosmos`` Astronomer Cosmos behavior. They can be set in the ``airflow.cfg file`` or using environment variables.

.. note::
    For more information, see `Setting Configuration Options <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`_.

**Sections:**

- [cosmos]
- [openlineage]

[cosmos]
~~~~~~~~

.. _cache_dir:

`cache_dir`_:
    The directory used for caching Cosmos data.

    - Default: ``{TMPDIR}/cosmos_cache`` (where ``{TMPDIR}`` is the system temporary directory)
    - Environment Variable: ``AIRFLOW__COSMOS__CACHE_DIR``

.. _enable_cache:

`enable_cache`_:
    Enable or disable caching of Cosmos data.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE``

.. _enable_cache_dbt_ls:

`enable_cache_dbt_ls`_:
    Enable or disable caching of the dbt ls command in case using ``LoadMode.DBT_LS`` in an Airflow Variable.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS``

.. _enable_cache_partial_parse:

`enable_cache_partial_parse`_:
    Enable or disable caching of dbt partial parse files in the local disk.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_PARTIAL_PARSE``

.. _enable_cache_package_lockfile:

`enable_cache_package_lockfile`_:
    Enable or disable caching of dbt project package lockfile.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_PACKAGE_LOCKFILE``

.. _propagate_logs:

`propagate_logs`_:
    Whether to propagate logs in the Cosmos module.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__PROPAGATE_LOGS``

.. _dbt_docs_dir:

`dbt_docs_dir`_:
    The directory path for dbt documentation.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__DBT_DOCS_DIR``

.. _dbt_docs_conn_id:

`dbt_docs_conn_id`_:
    The connection ID for dbt documentation.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__DBT_DOCS_CONN_ID``

.. _enable_cache_profile:

`enable_cache_profile`_:
    Enable caching for the DBT profile.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_PROFILE``

.. _profile_cache_dir_name:

`profile_cache_dir_name`_:
    Folder name to store the DBT cached profiles. This will be a sub-folder of ``cache_dir``

    - Default: ``profile``
    - Environment Variable: ``AIRFLOW__COSMOS__PROFILE_CACHE_DIR_NAME``

.. `virtualenv_max_retries_lock`_:
    When using ``ExecutionMode.VIRTUALENV`` and persisted virtualenv directories (`virtualenv_dir` argument),
    users can define how many seconds Cosmos waits for the lock to be released.

    - Default: 120
    - Environment Variable: ``AIRFLOW__COSMOS__VIRTUALENV_MAX_RETRIES_LOCK``

.. _remote_cache_dir:

`remote_cache_dir`_:
    The remote directory to store the dbt cache. Starting with Cosmos 1.6.0, you can store the `dbt ls` output as cache
    in a remote location (an alternative to the Variable cache approach released previously since Cosmos 1.5.0)
    using this configuration. The value for the remote cache directory can be any of the schemes that are supported by
    the `Airflow Object Store <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html>`_
    feature introduced in Airflow 2.8.0 (e.g. ``s3://your_s3_bucket/cache_dir/``, ``gs://your_gs_bucket/cache_dir/``,
    ``abfs://your_azure_container/cache_dir``, etc.)

    This is an experimental feature available since Cosmos 1.6 to gather user feedback and will be merged into the
    ``cache_dir`` setting in upcoming releases.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__REMOTE_CACHE_DIR``

.. _remote_cache_dir_conn_id:

`remote_cache_dir_conn_id`_:
    The connection ID for the remote cache directory. If this is not set, the default Airflow connection ID identified
    for the scheme will be used.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__REMOTE_CACHE_DIR_CONN_ID``


[openlineage]
~~~~~~~~~~~~~

.. _namespace:

`namespace`_:
    The OpenLineage namespace for tracking lineage.

    - Default: If not configured in Airflow configuration, it falls back to the environment variable ``OPENLINEAGE_NAMESPACE``, otherwise it uses ``DEFAULT_OPENLINEAGE_NAMESPACE``.
    - Environment Variable: ``AIRFLOW__OPENLINEAGE__NAMESPACE``

.. note::
    For more information, see `Openlieage Configuration Options <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html>`_.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

.. _LINEAGE_NAMESPACE:

`LINEAGE_NAMESPACE`_:
    The OpenLineage namespace for tracking lineage.

    - Default: If not configured in Airflow configuration, it falls back to the environment variable ``OPENLINEAGE_NAMESPACE``, otherwise it uses ``DEFAULT_OPENLINEAGE_NAMESPACE``.
