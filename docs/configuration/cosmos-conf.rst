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
