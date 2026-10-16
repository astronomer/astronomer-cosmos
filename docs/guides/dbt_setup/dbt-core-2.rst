.. _dbt_core_2:

dbt Core 2.0 support
====================

.. versionadded:: 1.16

`dbt Core 2.0 <https://github.com/dbt-labs/dbt-core>`_ is the Apache-2.0 release of the Rust engine that also powers :ref:`dbt Fusion <dbt_fusion>`.
It is published on PyPI as ``dbt-core`` (also ``dbt-oss`` and, from 2.0.0 on, ``dbt``), requires Python 3.11 or later, and bundles its own adapters.

Cosmos runs dbt Core 2.0 through the ``dbt`` binary, with ``InvocationMode.SUBPROCESS`` for rendering and execution.

Why subprocess only
~~~~~~~~~~~~~~~~~~~

dbt Core 2.0 ships a Python ``dbt`` package, but not the dbt Core 1.x API that ``InvocationMode.DBT_RUNNER`` relies on:

- ``dbt.version`` does not exist;
- ``dbtRunner(callbacks=...)`` raises ``NotImplementedError``, so the event callbacks ``ExecutionMode.WATCHER`` uses in ``DBT_RUNNER`` mode are unavailable;
- ``dbtRunner().invoke(["ls", "--output", "json"])`` returns node names instead of JSON records.

Cosmos therefore treats dbt Core 2.0 as "no dbt runner available": ``InvocationMode`` auto-detection picks ``SUBPROCESS``, and an explicit ``InvocationMode.DBT_RUNNER`` fails with an error that points at ``SUBPROCESS``.

Support
~~~~~~~

- :ref:`ExecutionMode.LOCAL <local-execution>` and :ref:`ExecutionMode.WATCHER <watcher-execution-mode>` with ``InvocationMode.SUBPROCESS``
- ``LoadMode.DBT_LS`` with ``RenderConfig(invocation_mode=InvocationMode.SUBPROCESS)``
- ``LoadMode.DBT_MANIFEST`` with a ``manifest.json`` written by dbt Core 2.0

Not supported: ``InvocationMode.DBT_RUNNER`` and :ref:`ExecutionMode.AIRFLOW_ASYNC <async-execution-mode>`.

How to use
~~~~~~~~~~

1. Install dbt Core 2.0 next to Airflow, or in its own virtualenv. Python 3.11 or later is required, and Postgres is an experimental adapter on the 2.0 engine (``DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true``):

   .. code-block:: bash

       pip install "dbt-core>=2.0.0rc2,<3"

2. Point ``RenderConfig`` and ``ExecutionConfig`` at the ``dbt`` binary and use ``InvocationMode.SUBPROCESS`` in both:

   .. code-block:: python

       from cosmos import DbtDag, ExecutionConfig, RenderConfig
       from cosmos.constants import ExecutionMode, InvocationMode

       DbtDag(
           ...,
           render_config=RenderConfig(
               dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
               invocation_mode=InvocationMode.SUBPROCESS,
           ),
           execution_config=ExecutionConfig(
               execution_mode=ExecutionMode.WATCHER,
               dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
               invocation_mode=InvocationMode.SUBPROCESS,
           ),
       )

   ``RenderConfig.invocation_mode`` defaults to ``InvocationMode.DBT_RUNNER``; with dbt Core 2.0 installed in the same environment as Airflow, rendering falls back to a subprocess and logs that it did. Set ``SUBPROCESS`` explicitly to make the choice visible.

Limitations
~~~~~~~~~~~

- dbt Core 2.0 is a release candidate at the time of writing (``2.0.0rc2``, September 2026); Cosmos tests against it in CI on duckdb.
- The adapters are the ones bundled in the 2.0 engine; check `the dbt documentation <https://docs.getdbt.com/docs/supported-data-platforms>`_ for your platform.
