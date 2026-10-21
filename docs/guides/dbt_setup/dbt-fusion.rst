.. _dbt_fusion:

dbt v2 support
==============

.. note::
    Available since Cosmos 1.11.0 when using ``ExecutionMode.LOCAL``.

Context
~~~~~~~

dbt Labs `launched <https://www.getdbt.com/blog/dbt-launch-showcase-2025-recap>`_ `dbt Fusion <https://github.com/dbt-labs/dbt-fusion>`_ on 28 May 2025.
It is a next-generation dbt engine, implemented in Rust instead of Python, that enables real-time model validation,
improved SQL parsing and state-aware orchestration.

With the general availability of dbt v2, dbt Labs renamed the dbt Fusion engine to **dbt**, and dbt Core v2 to **dbt OSS**
(see `Comparing dbt and dbt OSS <https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss>`_). This page uses the
current names and mentions the previous ones where it helps.

What dbt 2 is
~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Name
     - Previously
     - Licence
     - Installed with
   * - ``dbt``
     - dbt Fusion engine
     - Hybrid open source and commercial
     - ``pip install dbt``
   * - ``dbt-oss``
     - dbt Core v2
     - Apache 2.0
     - ``pip install dbt-oss``
   * - ``dbt-core`` 2.x
     - dbt Core v2
     - Apache 2.0 (installs dbt OSS)
     - ``pip install dbt-core``
   * - dbt Fusion binary
     - dbt Fusion
     - Hybrid open source and commercial
     - The `dbt installer <https://github.com/dbt-labs/dbt-fusion>`_, which downloads a standalone executable

dbt Core 1.x (versions lower than 2.0) is the previous, Python-based engine and is documented elsewhere.
All of the above share the same language specification, so the same dbt project can be used with each of them.
Refer to the `dbt documentation <https://docs.getdbt.com/docs/supported-data-platforms>`_ for the data platforms each one supports.

Supported setups
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Setup
     - How Cosmos runs dbt
     - Execution modes
     - Tested in Cosmos CI
   * - dbt Fusion binary
     - Subprocess. ``dbt`` is not importable from Airflow's Python environment, so ``dbtRunner`` is not used.
     - ``ExecutionMode.LOCAL`` and ``ExecutionMode.WATCHER``
     - Yes (matrix value ``2.0-dbt-fusion``): Snowflake and BigQuery
   * - ``dbt`` (``pip install dbt``)
     - ``InvocationMode.SUBPROCESS`` recommended
     - ``ExecutionMode.LOCAL``
     - No dedicated job yet
   * - ``dbt-oss`` (``pip install dbt-oss``)
     - ``InvocationMode.SUBPROCESS`` recommended
     - ``ExecutionMode.LOCAL``
     - No dedicated job yet

Cosmos's CI does not yet cover running tasks through ``dbtRunner`` on dbt 2 installed with ``pip``, so
set ``invocation_mode=InvocationMode.SUBPROCESS`` for pip installs.

How to use
~~~~~~~~~~

Install Cosmos 1.11.0 or later:

.. code-block:: bash

    pip install "astronomer-cosmos>=1.11.0"

**With the dbt Fusion binary**

Install the binary in your Airflow deployment. On Astro, add the following lines to your ``Dockerfile``, replacing
``<version>`` with the dbt version you want to pin:

.. code-block:: dockerfile

    USER root
    RUN apt-get update && apt-get install -y curl
    USER astro
    RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update --version <version>

Then point your ``DbtDag`` or ``DbtTaskGroup`` at the binary:

.. code-block:: python

    DbtDag(
        ...,
        execution_config=ExecutionConfig(dbt_executable_path="/home/astro/.local/bin/dbt"),
    )

**With ``pip``**

Install ``dbt`` or ``dbt-oss`` in the Python environment where Airflow runs, and use the subprocess invocation mode:

.. code-block:: python

    DbtDag(
        ...,
        execution_config=ExecutionConfig(invocation_mode=InvocationMode.SUBPROCESS),
    )

Limitations
~~~~~~~~~~~

- Cosmos does not support dbt 2 when using :ref:`ExecutionMode.AIRFLOW_ASYNC <async-execution-mode>`.
- dbt 2 has no Postgres adapter.

To support dbt 2, Cosmos modified its interaction with dbt. This works with the latest versions of dbt-core, but may not work with older versions. If you want to continue using dbt-core and were affected, set the environment variable ``AIRFLOW__COSMOS__PRE_DBT_FUSION=1`` and Cosmos interaction with dbt-core will work as previous versions.
