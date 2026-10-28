.. _dbt_oss:

dbt OSS support (experimental)
==============================

.. note::
    Available since Cosmos 1.16.0.

.. warning::
    Support for dbt OSS is experimental. The behavior may change between Cosmos releases, and it is not recommended for production yet. Cosmos CI runs the integration tests against BigQuery, and only the Databricks metric view and Snowflake semantic view tests against the other adapters (see `#3101 <https://github.com/astronomer/astronomer-cosmos/issues/3101>`_).

Context
~~~~~~~

dbt OSS is the open-source (Apache 2.0) distribution of dbt 2. See `Comparing dbt and dbt OSS <https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss>`_ for how it differs from dbt Core 1.x and :ref:`dbt Fusion <dbt_fusion>`.

It is published on PyPI as ``dbt-oss`` and includes its adapters, so you do not install ``dbt-<adapter>`` packages. See the `dbt supported data platforms <https://docs.getdbt.com/docs/supported-data-platforms>`_ for the adapters it supports. Adapters that dbt OSS marks as experimental, such as Postgres, need ``DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true``. Of these, Cosmos tests BigQuery, and Databricks and Snowflake for metric and semantic views only.

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

   Cosmos detects dbt OSS from the Python environment that Airflow runs in. It does not look at ``dbt_executable_path``. It treats the environment as dbt 2 when ``dbt-oss``, ``dbt-core`` or ``dbt`` is installed at version 2, so installing ``dbt`` 2.x also takes the dbt OSS code paths.

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
   * - Watcher modes, upstream failure
     - dbt OSS reports the nodes it skips after an upstream failure with the ``MarkSkippedChildren`` event, not the dbt 1.x events. Cosmos reads that event from the producer log, so the consumer tasks of those nodes run when the upstream node succeeds on a retry. This applies to ``ExecutionMode.WATCHER``, ``WATCHER_KUBERNETES`` and ``WATCHER_GCP_GKE``. It was checked against recorded dbt OSS events, and the retry scenario is not run end to end against dbt OSS in CI yet (see `#3137 <https://github.com/astronomer/astronomer-cosmos/issues/3137>`_).
   * - Test warnings and failures (``InvocationMode.SUBPROCESS``)
     - Reads the text output of dbt OSS to count test warnings for ``on_warning_callback`` and to name the failing tests in the error.
   * - ``inlets`` and ``outlets``
     - dbt OSS leaves ``args.profiles_dir`` out of ``run_results.json``. With ``openlineage-integration-common`` older than 1.44.0, this stops the ``inlets`` and ``outlets`` from being filled. Cosmos adds the value from ``--profiles-dir`` or ``DBT_PROFILES_DIR``.
   * - dbt docs
     - ``dbt docs generate`` writes a different site, without ``catalog.json``. ``DbtDocsS3Operator``, ``DbtDocsAzureStorageOperator`` and ``DbtDocsGCSOperator`` upload its ``index.html``, ``manifest.json``, ``assets/`` and ``info_schema/``.
   * - ``--vars`` and ``--args``
     - Cosmos writes dict values for these flags as one-line flow YAML, for example ``{var: '2'}``, because dbt OSS rejects block YAML that contains quoted strings. It does this for every dbt version, so only the flag text in task logs and rendered commands differs from before. See `#3099 <https://github.com/astronomer/astronomer-cosmos/pull/3099>`_.
   * - ``ExecutionMode.VIRTUALENV``
     - Add dbt OSS to ``py_requirements``, for example ``["dbt-oss~=2.0.0"]``. Cosmos checks the Airflow environment, not the virtual environment, so the ``inlets`` and ``outlets`` handling above also needs dbt OSS in the Airflow environment.
   * - Container modes, working directory (``ExecutionMode.KUBERNETES``, ``WATCHER_KUBERNETES``)
     - Start the pod in the dbt project directory and keep its ``profiles.yml`` there. dbt OSS looks for ``profiles.yml`` in the working directory before ``~/.dbt``, and its BigQuery adapter reads seed CSV files relative to the working directory, not ``--project-dir``. ``KubernetesPodOperator`` has no ``working_dir`` argument, so set ``WORKDIR`` in the image, or pass an entrypoint script that changes into the project directory through ``cmds``. See :ref:`kubernetes`.

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
   * - Projects with the same name (``InvocationMode.DBT_RUNNER``)
     - dbt OSS keeps the macros of a project by package name for the lifetime of the Python process, so two dbt projects with the same ``name`` that run in one worker process can resolve each other's macros. Give each project a unique ``name``. See `#3116 <https://github.com/astronomer/astronomer-cosmos/pull/3116>`_.
   * - Source freshness warnings in ``ExecutionMode.KUBERNETES``
     - In Cosmos 1.16.0rc1, ``on_warning_callback`` of ``DbtSourceKubernetesOperator`` and of the GKE source operator does not run for a stale source, because it looks for the dbt 1.x ``WARN freshness of`` line and dbt OSS prints ``Warned source``. See `#3156 <https://github.com/astronomer/astronomer-cosmos/pull/3156>`_.
   * - ``ExecutionMode.AIRFLOW_ASYNC``
     - Not supported or verified with dbt OSS. See `#3103 <https://github.com/astronomer/astronomer-cosmos/issues/3103>`_.
   * - Configuration that dbt 1.x accepts
     - dbt OSS rejects some of it. Cosmos CI runs ``dbt-autofix deprecations`` on the example projects, and you may need to run it on yours.
   * - dbt-loom
     - `dbt-loom <https://github.com/adrienneyager/dbt-loom>`_ is reported not to work with dbt OSS, which blocks the plugin hook it uses. See `dbt-loom issue 192 <https://github.com/adrienneyager/dbt-loom/issues/192>`_. The dbt-loom example DAGs are disabled in the dbt OSS CI job (`#3105 <https://github.com/astronomer/astronomer-cosmos/issues/3105>`_).

Tracking
~~~~~~~~

dbt OSS work is tracked in the `dbt-oss tracking issue <https://github.com/astronomer/astronomer-cosmos/issues/3101>`_. Related issues and pull requests have the `dbt-oss-v2 label <https://github.com/astronomer/astronomer-cosmos/labels/dbt-oss-v2>`_.
