.. _managing-semantic-layer:

Managing semantic layer
=======================

Cosmos can render adapter-native semantic layer objects — Databricks Unity Catalog metric views and
Snowflake semantic views — as their own ``semantic_layer`` resource type, instead of a regular ``model``.

.. note::
    This is about **provider-native** semantic layer objects, not dbt's own Metric Flow. dbt Metric
    Flow resources (``semantic_model``, ``metric``, ``saved_query``) are untouched and unaffected.

Prerequisites
~~~~~~~~~~~~~

- **Databricks:** `dbt-databricks <https://pypi.org/project/dbt-databricks/>`_ >= 1.12
- **Snowflake:** the `Snowflake-Labs/dbt_semantic_view <https://github.com/Snowflake-Labs/dbt_semantic_view>`_ package

Without one of these, semantic layer models stay plain ``model`` nodes.

How it works
~~~~~~~~~~~~

dbt has no distinct resource type for these objects — they're plain ``model`` nodes with
``config.materialized: metric_view`` (Databricks) or ``semantic_view`` (Snowflake). Cosmos detects
that materialization at parse time and reclassifies the node as ``semantic_layer``, which changes:

- **Task identity:** a ``_semantic_layer`` task name suffix and a ``DbtSemantic*`` operator instead of ``DbtRun*``. The underlying dbt command is still ``dbt run``.
- **TestBehavior.BUILD:** collapses into ``DbtBuild``, same as any other buildable resource.
- **Tests:** still render under the default ``TestBehavior.AFTER_EACH``.
- **Selectors:** ``resource_type:model`` no longer matches it — use ``resource_type:semantic_layer``.

Example
~~~~~~~

.. code-block:: sql+jinja

    {{ config(materialized='metric_view') }}

    select * from {{ ref('customers') }}

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig

    semantic_layer_only = DbtTaskGroup(
        render_config=RenderConfig(select=["resource_type:semantic_layer"]),
    )

See :doc:`Selecting & Excluding <selecting-excluding>` for the full selector reference.
