.. _managing-semantic-metric-views:

Managing semantic metric views
==============================

Cosmos can render adapter-native semantic metric views (Databricks Unity Catalog metric views and
Snowflake semantic views) as their own ``semantic_metric_view`` resource type, instead of a regular ``model``.

This is opt-in. Enable it with the :ref:`enable_semantic_metric_view_reclassification` setting
(``AIRFLOW__COSMOS__ENABLE_SEMANTIC_METRIC_VIEW_RECLASSIFICATION=True``).

Prerequisites
~~~~~~~~~~~~~

- **Databricks:** `dbt-databricks <https://pypi.org/project/dbt-databricks/>`_ >= 1.12.5
- **Snowflake:** the `Snowflake-Labs/dbt_semantic_view <https://github.com/Snowflake-Labs/dbt_semantic_view>`_ package

How it works
~~~~~~~~~~~~

dbt has no distinct resource type for these objects. They are ``model`` nodes with
``config.materialized: metric_view`` (Databricks) or ``semantic_view`` (Snowflake). Once enabled, Cosmos detects
that materialization at parse time and reclassifies the node as ``semantic_metric_view``, which changes:

- **Task identity:** a ``_semantic_metric_run`` task name suffix and a ``DbtSemanticMetricView*`` operator instead of
  ``DbtRun*``. The underlying dbt command is still ``dbt run``.
- **TestBehavior.BUILD:** collapses into ``DbtBuild``, same as any other buildable resource.
- **Selectors:** ``resource_type:model`` no longer matches it. Use ``resource_type:semantic_metric_view`` instead. This
  selector only works when Cosmos itself performs node selection (e.g. ``LoadMode.DBT_MANIFEST``). Under
  ``LoadMode.DBT_LS``, ``select``/``exclude`` are passed directly to ``dbt ls``, which has no
  ``semantic_metric_view`` resource type, so use ``config.materialized:metric_view`` or
  ``config.materialized:semantic_view`` instead.

Testing
~~~~~~~

Cosmos renders test tasks for semantic metric views like for any other model. Generic tests work on Databricks metric
views. On Snowflake, tests that aggregate over a column, such as ``accepted_values``, fail on semantic views. Use
singular tests written with ``SEMANTIC_VIEW(...)`` instead.

Example
~~~~~~~

.. code-block:: sql+jinja

    {{ config(materialized='metric_view') }}

    select * from {{ ref('customers') }}

.. code-block:: python

    from cosmos import DbtTaskGroup, LoadMode, RenderConfig

    # LoadMode.DBT_MANIFEST (or another mode where Cosmos applies selection itself)
    semantic_metric_views_only = DbtTaskGroup(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST, select=["resource_type:semantic_metric_view"]
        ),
    )

    # LoadMode.DBT_LS: select/exclude are passed straight to `dbt ls`, which has no `semantic_metric_view`
    # resource type, so filter on the underlying materialization instead
    semantic_metric_views_only_dbt_ls = DbtTaskGroup(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, select=["config.materialized:metric_view"]
        ),
    )

See :doc:`Selecting & Excluding <selecting-excluding>` for the full selector reference.
