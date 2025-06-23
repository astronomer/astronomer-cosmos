.. _dbt_fusion:

dbt Fusion support
==================

.. note::
    Available since Cosmos 1.11 when using ``ExecutionMode.LOCAL``.

Context
-------

dbt Labs `launched <https://www.getdbt.com/blog/dbt-launch-showcase-2025-recap>`_ `dbt Fusion <https://github.com/dbt-labs/dbt-fusion>`_ on 28 May 2025.
It is a unified approach that aims to merge dbt Core and dbt Cloud features via a completely new CLI,
implemented in a different programming language (Rust, as opposed to Python).
As part of this, dbt Labs are `rewriting all dbt adaptors <https://github.com/dbt-labs/dbt-fusion/tree/main/crates/dbt-fusion-adapter/src/adapters>`_ (equivalent to Airflow providers) in Rust, starting from Snowflake.
They are also changing the `licensing model <https://github.com/dbt-labs/dbt-fusion/blob/main/LICENSES.md>`_ to a hybrid Open-Source and commercial license, which has implications for Cosmos.
Among the advantages of using dbt Fusion is a significant performance improvement (claimed to be 30x, for some models) and AI features.

Support
-------

Cosmos 1.11 adds initial support to running dbt Fusion with Cosmos when using ``ExecutionMode.LOCAL``.

We do not have a solution for using `ExecutionMode.AIRFLOW_ASYNC <https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html#airflow-async>`_.

How to use
----------

1. Install dbt Fusion in your Airflow deployment

End-users should install the dbt Fusion package themselves. An example of how to do this in Astro would be to add the following lines in your ``Dockerfile``:

```
USER root
RUN apt install -y curl
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
```

2. Update your ``DbtDag`` or ``DbtTaskGroup`` to use the dbt Fusion binary

Example:

```
DbtDag(
    ...,
    execution_config=ExecutionConfig(
        dbt_executable_path="/home/astro/.local/bin/dbt"
    )
)
```

Limitations
-----------

- Currently (23 June 2025) dbt Fusion is still in beta
- dbt Fusion only supports Snowflake
- Cosmos does not support dbt Fusion when using ``ExecutionMode.AIRFLOW_ASYNC``
- To support dbt Fusion, Cosmos changed how it interacts with dbt. This works with the latest versions of dbt-core, but may not
work with older versions. If you want to continue using dbt-core and was affected, set the environment variable
``AIRFLOW__COSMOS__PRE_DBT_FUSION=1`` and Cosmos interaction with dbt-core will work as previous versions.
