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

    - Default: ``{TMPDIR}/cosmos`` (where ``{TMPDIR}`` is the system temporary directory)
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

`enable_cache_dbt_yaml_selectors`_:
    Enable or disable caching of the YAML selectors in case using ``LoadMode.DBT_MANIFEST`` with ``RenderConfig.selector`` in an Airflow Variable.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS``

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

.. _dbt_docs_projects:

`dbt_docs_projects`_:
    (Introduced in Cosmos 1.11.0; applicable to Airflow >= 3.1): JSON mapping configuring one or more dbt docs projects for the Airflow 3 UI plugin.

    Structure: mapping of slug to a dict with keys ``dir`` (required), ``index`` (optional, default ``index.html``),
    ``name`` (optional, label in the menu), and ``conn_id`` (optional connection to read remote storage).
    A "slug" here means a short, URL-safe identifier you choose for each docs project. It's used in the path segment
    ``/cosmos/<slug>/…`` and in the UI menu label mapping. Prefer lowercase letters, numbers, and hyphens/underscores (e.g., core, mart, jaffle-shop).


    Example:

    .. code-block:: ini

       [cosmos]
       dbt_docs_projects = {
         "core": {"dir": "/path/to/core/target", "index": "index.html", "name": "dbt Docs (Core)"},
         "mart": {"dir": "s3://bucket/path/to/mart/target", "conn_id": "aws_default", "name": "dbt Docs (Mart)"}
       }

    Environment Variable: ``AIRFLOW__COSMOS__DBT_DOCS_PROJECTS``

    .. code-block:: bash

       export AIRFLOW__COSMOS__DBT_DOCS_PROJECTS='{"core":{"dir":"/path/to/core/target","index":"index.html","name":"dbt Docs (Core)"},"mart":{"dir":"s3://bucket/path/to/mart/target","conn_id":"aws_default","name":"dbt Docs (Mart)"}}'

.. _dbt_docs_dir:

`dbt_docs_dir`_:
    (Applicable to Airflow 2): The directory path for dbt documentation.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__DBT_DOCS_DIR``

.. _dbt_docs_conn_id:

`dbt_docs_conn_id`_:
    (Applicable to Airflow 2): The connection ID for dbt documentation.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__DBT_DOCS_CONN_ID``

.. _default_copy_dbt_packages:

`default_copy_dbt_packages`_:
    (Introduced in Cosmos 1.10.0):  By default, Cosmos 1.x either installs ``dbt deps`` or creates a symbolic link to the original ``dbt_packages`` folder.
    This configuration changes this behaviour, by copying the dbt project ``dbt_packages`` instead of creating symbolic links, so Cosmos can run ``dbt deps`` incrementally.
    Can be overridden at a ``DbtDag`` and ``DbtTaskGroup``, via ``ProjectConfig.copy_dbt_packages``, or at an operator level, via ``operator_args={"copy_dbt_packages"}``.

    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__DEFAULT_COPY_DBT_PACKAGES``

.. _enable_cache_profile:

`enable_cache_profile`_:
    Enable caching for the DBT profile.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_CACHE_PROFILE``

.. _pre_dbt_fusion:
    From Cosmos 1.11, we have introduced support for dbt Fusion. Some of the changes may not be compatible with legacy versions of dbt-core.
    If you find any issues on how Cosmos interacts with older versions of dbt-core you can use this configuration.

    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__PRE_DBT_FUSION``

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
    feature (e.g. ``s3://your_s3_bucket/cache_dir/``, ``gs://your_gs_bucket/cache_dir/``,
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

.. _remote_target_path:

`remote_target_path`_:
    (Introduced since Cosmos 1.7.0) The path to the remote target directory. This is the directory designated to
    remotely copy & store in the files generated and stored by dbt in the dbt project's target directory.
    While this remote path is intended to copy files from the dbt project’s target directory, Cosmos currently only
    supports copying files from the ``compiled`` directory within the ``target`` folder — and only when the execution
    mode is set to ``ExecutionMode.AIRFLOW_ASYNC``. Future releases will add support for copying additional files from
    the target directory.
    The value for the remote target path can be any of the schemes that are supported by the
    `Airflow Object Store <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html>`_
    feature (e.g. ``s3://your_s3_bucket/target_dir/``, ``gs://your_gs_bucket/target_dir/``,
    ``abfs://your_azure_container/cache_dir``, etc.)

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__REMOTE_TARGET_PATH``

.. _remote_target_path_conn_id:

`remote_target_path_conn_id`_:
    (Introduced since Cosmos 1.7.0) The connection ID for the remote target path. If this is not set, the default
    Airflow connection ID identified for the scheme will be used.

    - Default: ``None``
    - Environment Variable: ``AIRFLOW__COSMOS__REMOTE_TARGET_PATH_CONN_ID``

.. _enable_setup_async_task:

`enable_setup_async_task`_:
    (Introduced in Cosmos 1.9.0): Enables a setup task for ``ExecutionMode.AIRFLOW_ASYNC`` to generate SQL files and upload them to a remote location (S3/GCS), preventing the ``run`` command from being executed on every node. You need to specify ``remote_target_path_conn_id`` and ``remote_target_path`` configuration to upload the artifact to the remote location.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_SETUP_ASYNC_TASK``

.. _enable_teardown_async_task:

`enable_teardown_async_task`_:
    (Introduced in Cosmos 1.9.0): Enables a teardown task for ``ExecutionMode.AIRFLOW_ASYNC`` to delete the SQL files from remote location (S3/GCS). You need to specify ``remote_target_path_conn_id`` and ``remote_target_path`` configuration to delete the artifact from the remote location.

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_TEARDOWN_ASYNC_TASK``

.. _upload_sql_to_xcom:

`upload_sql_to_xcom`_:
    (Introduced in Cosmos 1.11.0): Enable this if the setup async task is enabled for ``ExecutionMode.AIRFLOW_ASYNC`` and you want to upload the compiled SQL to Airflow XCom instead of a remote location (e.g., S3 or GCS).

    - Default: ``True``
    - Environment Variable: ``AIRFLOW__COSMOS__UPLOAD_SQL_TO_XCOM``

.. _use_dataset_airflow3_uri_standard:

`use_dataset_airflow3_uri_standard`_:
    (Introduced in Cosmos 1.10.0): Changes Cosmos Dataset (Asset) URIs to be Airflow 3 compliant. Since this would be a breaking change, it is False by default in Cosmos 1.x.
    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD``

.. _enable_memory_optimised_imports:

`enable_memory_optimised_imports`_:
    (Introduced in Cosmos 1.10.1): Eager imports in cosmos/__init__.py expose all Cosmos classes at the top level,
    which can significantly increase memory usage—even when Cosmos is just installed but not actively used. This option allows
    disabling those eager imports to reduce memory footprint. When enabled, users must access Cosmos classes via their full
    module paths, avoiding the overhead of importing unused modules and classes.

    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_MEMORY_OPTIMISED_IMPORTS``

    .. note::
        This option will become the default behavior in Cosmos 2.0.0, where all eager imports will be removed from ``cosmos/__init__.py``.

    As an example, when this option is enabled, the following is an example of specifying the imports with full module paths:

    .. literalinclude:: ../../dev/dags/basic_cosmos_dag_full_module_path_imports.py
        :language: python
        :start-after: [START cosmos_explicit_imports]
        :end-before: [END cosmos_explicit_imports]

    as opposed to the following approach you might have when this option is disabled (default):

    .. literalinclude:: ../../dev/dags/basic_cosmos_dag.py
        :language: python
        :start-after: [START cosmos_init_imports]
        :end-before: [END cosmos_init_imports]

.. _enable_orjson_parser:

`enable_orjson_parser`_:
    (EXPERIMENTAL - Introduced in Cosmos 1.14.0): Enable the use of ``orjson`` for **all** JSON operations
    across Cosmos instead of the standard ``json`` library. ``orjson`` provides significantly faster JSON
    serialization and deserialization, which can reduce DAG parsing time for large dbt projects.
    Requires installing orjson via ``astronomer-cosmos[orjson]``.

    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_ORJSON_PARSER``

    .. note::
        This is an experimental feature. If enabled without installing orjson, Cosmos will raise an
        ``ImportError`` with installation instructions.

.. _enable_debug_mode:

`enable_debug_mode`_:
    Enable or disable debug mode. When enabled, Cosmos will track memory utilization for its tasks and push the peak
    memory usage (in MB) to XCom under the key ``cosmos_debug_max_memory_mb``. This is useful for profiling and
    optimizing resource allocation for dbt tasks. Requires ``psutil`` to be installed.

    - Default: ``False``
    - Environment Variable: ``AIRFLOW__COSMOS__ENABLE_DEBUG_MODE``

.. _debug_memory_poll_interval_seconds:

`debug_memory_poll_interval_seconds`_:
    The interval (in seconds) at which memory utilization is polled when debug mode is enabled. Lower values provide
    more accurate peak memory measurements but may add slight overhead.

    - Default: ``0.5``
    - Environment Variable: ``AIRFLOW__COSMOS__DEBUG_MEMORY_POLL_INTERVAL_SECONDS``

[openlineage]
~~~~~~~~~~~~~

.. _namespace:

`namespace`_:
    The OpenLineage namespace for tracking lineage.

    - Default: If not configured in Airflow configuration, it falls back to the environment variable ``OPENLINEAGE_NAMESPACE``, otherwise it uses ``DEFAULT_OPENLINEAGE_NAMESPACE``.
    - Environment Variable: ``AIRFLOW__OPENLINEAGE__NAMESPACE``

.. note::
    For more information, see `OpenLineage Configuration Options <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html>`_.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

.. _LINEAGE_NAMESPACE:

`LINEAGE_NAMESPACE`_:
    The OpenLineage namespace for tracking lineage.

    - Default: If not configured in Airflow configuration, it falls back to the environment variable ``OPENLINEAGE_NAMESPACE``, otherwise it uses ``DEFAULT_OPENLINEAGE_NAMESPACE``.
