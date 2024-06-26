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

.. enable_cache_dbt_ls:

`enable_cache_dbt_ls`_:
    Enable or disable caching of the dbt ls command in case using ``LoadMode.DBT_LS`` in an Airflow Variable.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS``

.. _enable_cache_partial_parse:

`enable_cache_partial_parse`_:
    Enable or disable caching of dbt partial parse files in the local disk.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_PARTIAL_PARSE``

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
