.. _dbt_oss:

dbt OSS support
===============

.. note::
    Available since Cosmos 1.16.0.

Context
~~~~~~~

dbt OSS is the open-source distribution of dbt. See `Comparing dbt and dbt OSS <https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss>`_ for how it relates to dbt Core 1.x and :ref:`dbt Fusion <dbt_fusion>`.

dbt OSS is published on PyPI as ``dbt-oss``. It includes its adapters: dbt-oss 2.0.8 lists Snowflake, BigQuery, Databricks, Redshift, DuckDB, Salesforce and ClickHouse as supported, and treats other adapters, such as Postgres, as experimental (they need ``DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true``).

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

   To run a dbt installation other than the one in the Airflow environment, set ``ExecutionConfig.dbt_executable_path`` (and ``RenderConfig.dbt_executable_path`` when parsing with ``LoadMode.DBT_LS``). Cosmos detects dbt OSS from the Python environment Airflow runs in and does not inspect the executable path, so also set ``InvocationMode.SUBPROCESS`` in that case.

How Cosmos behaves with dbt OSS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``LoadMode.DBT_LS`` runs ``dbt ls`` in a subprocess, even under ``InvocationMode.DBT_RUNNER``, because the dbt OSS ``dbtRunner`` returns only node names for ``dbt ls --output json``. ``dbt deps`` still runs through ``dbtRunner``.
- ``ExecutionMode.WATCHER`` runs the producer task in a subprocess. The dbt OSS ``dbtRunner`` does not accept the event callbacks the producer uses. Setting ``InvocationMode.DBT_RUNNER`` explicitly for the producer raises ``CosmosValueError``.
- Node statuses are matched case-insensitively, since dbt OSS reports some of them capitalised (for example ``Warn`` for a source freshness check).
- Missing packages are installed by dbt during ``dbt ls``, even when ``install_dbt_deps`` is ``False``.
- Dictionary flags such as ``--vars`` are passed to dbt OSS as JSON, since it rejects the quoted string values of the YAML Cosmos writes for dbt Core 1.x.
- With ``InvocationMode.SUBPROCESS``, Cosmos reads the text output of dbt OSS to count test warnings and to name failing tests (``Summary:`` line, ``Warned test`` and ``Failed test`` lines).
- The dbt OSS ``run_results.json`` has no ``args.profiles_dir``. With ``openlineage-integration-common`` older than 1.44.0, which reads it without a fallback, Cosmos adds it from ``--profiles-dir`` or ``DBT_PROFILES_DIR`` before the OpenLineage artifact parser reads the file, so the ``inlets`` and ``outlets`` are populated.
- ``dbt docs generate`` writes a different site, without ``catalog.json``. ``DbtDocsS3Operator``, ``DbtDocsAzureStorageOperator`` and ``DbtDocsGCSOperator`` upload its ``index.html``, ``manifest.json``, ``assets/`` and ``info_schema/``.
- In ``ExecutionMode.WATCHER``, Cosmos uses the dbt graph to find the nodes dbt OSS skips after an upstream failure, so their consumer tasks are built when the upstream node succeeds on a retry.
- dbt OSS does not import ``dbt_*`` modules from the Python path, so the DAG-folder import protection for dbt Core 1.x plugin discovery (`#1673 <https://github.com/astronomer/astronomer-cosmos/issues/1673>`_) does not apply, and its tests are skipped.
- With ``ExecutionMode.VIRTUALENV``, install dbt OSS in the virtual environment, for example with ``py_requirements=["dbt-oss~=2.0.0"]``. The virtual environment is not used to detect dbt OSS, so Cosmos CI installs it in the Airflow environment as well.

Limitations
~~~~~~~~~~~

The list below reflects what is skipped or not covered in Cosmos CI for dbt OSS. The unit tests run against ``dbt-oss`` on Python 3.11 to 3.14, and the integration tests run against BigQuery on Python 3.11 and 3.12. Python 3.10 is not tested with dbt OSS.

Features that behave differently:

- **Partial parsing**: ``partial_parse`` has no effect, because dbt OSS does not write ``target/partial_parse.msgpack``. dbt OSS has its own opt-in partial parsing, which Cosmos does not use yet.
- **Event callbacks**: the dbt OSS ``dbtRunner`` does not support callbacks, and dbt does not plan to add them. The watcher producer cannot use them, and the error Cosmos raises for a failed ``run-operation`` does not include the macro error text with ``InvocationMode.DBT_RUNNER``.
- **Empty models**: ``dbt ls`` does not list empty models, so Cosmos does not render them with ``LoadMode.DBT_LS``.
- **Kubernetes watcher modes**: the handling of nodes skipped after an upstream failure applies to ``ExecutionMode.WATCHER`` only. ``WATCHER_KUBERNETES`` and ``WATCHER_GCP_GKE`` do not receive the dbt graph it relies on.

Not tested yet with dbt OSS:

- ``ExecutionMode.AIRFLOW_ASYNC``, which needs ``dbt-bigquery`` that cannot be installed next to dbt OSS.
- Kubernetes execution modes other than ``KubernetesOperator`` and ``WATCHER_KUBERNETES``.
- Cross-project references with `dbt-loom <https://github.com/nicholasyager/dbt-loom>`_.
- dbt Python models on Databricks.
- Adapters that dbt OSS treats as experimental, such as Postgres. The CI job uses BigQuery.
- Uploading the dbt OSS docs site to a real S3, Azure or GCS bucket, and viewing it in the Airflow dbt docs menu.
- ``DbtDocsS3KubernetesOperator``, which uploads the whole ``target`` folder from the pod.

Related work is tracked in the `dbt-oss CI tracking issue <https://github.com/astronomer/astronomer-cosmos/issues/3101>`_.
