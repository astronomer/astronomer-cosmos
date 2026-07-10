.. _managing-semantic-layer:

Managing semantic layer
=======================

.. note::
    This feature targets **adapter-native semantic layer objects**, not dbt Metric Flow. Your dbt project must include the relevant adapter support:

    - **Databricks:** `dbt-databricks <https://pypi.org/project/dbt-databricks/>`_ >= 1.12 (Unity Catalog `metric_view` materialization)
    - **Snowflake:** the community package `Snowflake-Labs/dbt_semantic_view <https://github.com/Snowflake-Labs/dbt_semantic_view>`_ installed in your dbt project (`semantic_view` materialization)

By default, Cosmos parses dbt models whose ``config.materialized`` is ``metric_view`` or ``semantic_view`` as a distinct ``semantic_layer`` resource type, rather than a regular ``model``. dbt itself does not expose these objects as a separate resource type — they are plain ``model`` nodes distinguished only by their materialization — so Cosmos reclassifies them at parse time to give them their own task identity, selectors, and operators.

This is **not** dbt Metric Flow. Cosmos's ``semantic_layer`` resource type does not cover dbt Metric Flow resources such as ``semantic_model``, ``metric``, or ``saved_query``. Those remain separate dbt resource types and are not reclassified as ``semantic_layer``.

- **Reclassification:** Cosmos applies reclassification when parsing via ``LoadMode.DBT_LS``, ``LoadMode.DBT_MANIFEST``, or the legacy ``LoadMode.CUSTOM`` path.
- **Operators:** Outside ``TestBehavior.BUILD``, semantic layer nodes are rendered as ``DbtSemantic`` operators (for example, ``DbtSemanticLocalOperator``). The underlying dbt command is ``dbt run``, identical to ``DbtRun``, but these tasks use a distinct operator class so they render separately in the Airflow UI and can evolve independently as adapter support matures across execution modes.
- **TestBehavior.BUILD:** Under ``BUILD``, semantic layer nodes are collapsed into ``DbtBuild`` tasks alongside models, seeds, and snapshots.
- **Tests:** Semantic layer nodes are included in Cosmos's testable resources, so tests attached to them are rendered under the default ``TestBehavior.AFTER_EACH`` behavior.

Example dbt model (Databricks metric view):

.. code-block:: sql

    {{ config(materialized='metric_view') }}

    select * from {{ ref('customers') }}

Example (select only semantic layer nodes):

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig

    semantic_layer_only = DbtTaskGroup(
        render_config=RenderConfig(
            select=["resource_type:semantic_layer"],
        )
    )


How Cosmos classifies semantic layer nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cosmos reclassifies a node to ``DbtResourceType.SEMANTIC_LAYER`` when all of the following are true:

- The node's dbt resource type is ``model``.
- The node's ``config.materialized`` is ``metric_view`` (Databricks Unity Catalog metric views) or ``semantic_view`` (Snowflake semantic views via ``dbt_semantic_view``).

Reclassification happens during project parsing, before Cosmos renders Airflow tasks. The reclassified resource type is used consistently across manifest parsing, ``dbt ls`` output parsing, and the legacy custom loader path.

Without the adapter prerequisites above, these nodes remain plain ``model`` nodes in Cosmos. They will not match ``resource_type:semantic_layer`` and will be rendered as regular ``DbtRun`` tasks.


Selecting semantic layer nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because Cosmos reclassifies these nodes out of ``model``, ``resource_type:model`` selectors no longer match them. Use ``resource_type:semantic_layer`` instead. You can also select by materialization (for example, ``config.materialized:metric_view``).

See :doc:`Selecting & Excluding <selecting-excluding>` for the full selector reference.


Limitations
~~~~~~~~~~~

- **Adapter prerequisites:** If your project does not use ``dbt-databricks`` >= 1.12 (Databricks) or the ``dbt_semantic_view`` package (Snowflake), Cosmos cannot distinguish semantic layer models from ordinary models at parse time.
- **Not Metric Flow:** dbt Metric Flow resources (``semantic_model``, ``metric``, ``saved_query``, and related nodes) are unaffected. Cosmos does not reclassify them as ``semantic_layer``.
- **Watcher mode:** Under ``ExecutionMode.WATCHER``, semantic layer nodes participate in stale-source exclusion alongside models, seeds, and snapshots. dbt itself has no ``semantic_layer`` resource type, so WATCHER's producer task still invokes ``dbt build`` with dbt's native resource types only; Cosmos handles semantic layer nodes on the consumer side of the WATCHER pattern.
- **TestBehavior.BUILD:** Semantic layer nodes run via ``dbt build`` under ``BUILD``, not as standalone ``DbtSemantic`` tasks.
