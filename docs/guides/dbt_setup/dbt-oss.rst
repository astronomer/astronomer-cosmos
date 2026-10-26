.. _dbt_oss:

dbt OSS support
===============

.. note::
    Available since Cosmos 1.16.0.

Context
~~~~~~~

dbt OSS is the open-source (Apache 2.0) distribution of dbt 2. See `Comparing dbt and dbt OSS <https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss>`_ for how it differs from dbt Core 1.x and :ref:`dbt Fusion <dbt_fusion>`.

It is published on PyPI as ``dbt-oss`` and includes its adapters, so you do not install ``dbt-<adapter>`` packages. dbt-oss 2.0.8 supports Snowflake, BigQuery, Databricks, Redshift, DuckDB, Salesforce and ClickHouse. Other adapters, such as Postgres, are experimental and need ``DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true``.

How to use
~~~~~~~~~~

1. Install dbt OSS in the environment where dbt runs

   dbt OSS and dbt Core 1.x both provide the ``dbt`` Python package, so install dbt OSS in an environment without dbt Core 1.x.

   .. code-block:: bash

       pip install "astronomer-cosmos[dbt-oss]>=1.16.0"

2. Use Cosmos as usual

   .. code-block:: python

       from cosmos import DbtDag, ProjectConfig, ProfileConfig

       DbtDag(
           project_config=ProjectConfig("/usr/local/airflow/dbt/my_project"),
           profile_config=ProfileConfig(
               profile_name="my_profile",
               target_name="dev",
               profiles_yml_filepath="/usr/local/airflow/dbt/my_project/profiles.yml",
           ),
           dag_id="my_dbt_oss_dag",
           ...,
       )

   Cosmos detects dbt OSS from the Python environment that Airflow runs in. It does not look at ``dbt_executable_path``.

How Cosmos behaves with dbt OSS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are the differences between dbt OSS and dbt Core 1.x that Cosmos accounts for.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Area
     - What Cosmos does
   * - DAG parsing (``LoadMode.DBT_LS``)
     - Runs ``dbt ls`` in a subprocess, even with ``InvocationMode.DBT_RUNNER``, because the dbt OSS ``dbtRunner`` returns only node names for ``dbt ls --output json``. ``dbt deps`` still runs through ``dbtRunner``.
   * - Missing dbt packages
     - dbt OSS installs them itself during ``dbt ls``, even when ``install_dbt_deps`` is ``False``.
   * - ``ExecutionMode.WATCHER``
     - Runs the producer task in a subprocess, because the ``dbtRunner`` of dbt OSS has no event callbacks. Setting ``InvocationMode.DBT_RUNNER`` for the producer raises ``CosmosValueError``.
   * - ``ExecutionMode.WATCHER``, upstream failure
     - dbt OSS reports the nodes it skips in a different way. Cosmos uses the dbt graph to find them, so their consumer tasks run when the upstream node succeeds on a retry.
   * - Test warnings and failures (``InvocationMode.SUBPROCESS``)
     - Reads the text output of dbt OSS to count test warnings for ``on_warning_callback`` and to name the failing tests in the error.
   * - ``inlets`` and ``outlets``
     - dbt OSS leaves ``args.profiles_dir`` out of ``run_results.json``. With ``openlineage-integration-common`` older than 1.44.0, this stops the ``inlets`` and ``outlets`` from being filled. Cosmos adds the value from ``--profiles-dir`` or ``DBT_PROFILES_DIR``.
   * - dbt docs
     - ``dbt docs generate`` writes a different site, without ``catalog.json``. ``DbtDocsS3Operator``, ``DbtDocsAzureStorageOperator`` and ``DbtDocsGCSOperator`` upload its ``index.html``, ``manifest.json``, ``assets/`` and ``info_schema/``.
   * - ``ExecutionMode.VIRTUALENV``
     - Add dbt OSS to ``py_requirements``, for example ``["dbt-oss~=2.0.0"]``. Cosmos checks the Airflow environment, not the virtual environment, so the ``inlets`` and ``outlets`` handling above also needs dbt OSS in the Airflow environment.

Limitations
~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Area
     - Limitation
   * - Partial parsing
     - ``partial_parse`` has no effect, because dbt OSS does not write ``target/partial_parse.msgpack``. dbt OSS has its own opt-in partial parsing, which Cosmos does not use.
   * - Event callbacks
     - The ``dbtRunner`` of dbt OSS has no callbacks, and dbt does not plan to add them. With ``InvocationMode.DBT_RUNNER``, the error for a failed ``run-operation`` does not include the macro error text.
   * - Empty models
     - ``dbt ls`` does not list empty models, so ``LoadMode.DBT_LS`` does not render them.
   * - Kubernetes watcher modes
     - ``WATCHER_KUBERNETES`` and ``WATCHER_GCP_GKE`` do not get the skipped-node handling above, because the scheduler cannot see the dbt version inside the pod.
   * - dbt-loom
     - `dbt-loom <https://github.com/nicholasyager/dbt-loom>`_ does not work with dbt OSS, which blocks the plugin hook it uses. See `dbt-loom issue 192 <https://github.com/adrienneyager/dbt-loom/issues/192>`_.

Tracking
~~~~~~~~

dbt OSS work is tracked in the `dbt-oss tracking issue <https://github.com/astronomer/astronomer-cosmos/issues/3101>`_. Related issues and pull requests have the `dbt-oss-v2 label <https://github.com/astronomer/astronomer-cosmos/labels/dbt-oss-v2>`_.
