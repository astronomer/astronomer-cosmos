.. _managing-semantic-layer:

Managing semantic layer
=======================

Cosmos can render adapter-native semantic layer objects (Databricks Unity Catalog metric views and
Snowflake semantic views) as their own ``semantic_layer`` resource type, instead of a regular ``model``.


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
- **Selectors:** ``resource_type:model`` no longer matches it — use ``resource_type:semantic_layer`` instead. This
  reclassification, and therefore this selector, is only applied when Cosmos itself performs node selection (e.g.
  ``LoadMode.DBT_MANIFEST``). Under ``LoadMode.DBT_LS``, ``select``/``exclude`` are passed directly to the ``dbt ls``
  command, dbt has no ``semantic_layer`` resource type, and these nodes remain plain ``model`` nodes to dbt — use
  ``config.materialized:metric_view`` or ``config.materialized:semantic_view`` instead.

Example
~~~~~~~

.. code-block:: sql+jinja

    {{ config(materialized='metric_view') }}

    select * from {{ ref('customers') }}

.. code-block:: python

    from cosmos import DbtTaskGroup, LoadMode, RenderConfig

    # LoadMode.DBT_MANIFEST (or another mode where Cosmos applies selection itself)
    semantic_layer_only = DbtTaskGroup(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST, select=["resource_type:semantic_layer"]
        ),
    )

    # LoadMode.DBT_LS: select/exclude are passed straight to `dbt ls`, which has no `semantic_layer`
    # resource type, so filter on the underlying materialization instead
    semantic_layer_only_dbt_ls = DbtTaskGroup(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, select=["config.materialized:metric_view"]
        ),
    )

See :doc:`Selecting & Excluding <selecting-excluding>` for the full selector reference.
