.. _source-nodes-rendering:

Source Nodes Rendering
======================

.. note::
    This feature is only available for dbt-core >= 1.5 and cosmos >= 1.6.0.

By default, Cosmos does not render dbt sources automatically. Instead, you need to configure the rendering of sources explicitly.
You can control this behavior using the ``source_rendering_behavior`` field in the ``RenderConfig`` object. This is how it works:

- **all**:
  When set to ``all``, Cosmos renders all sources in the dbt project. It uses three different node types for this:

    - ``EmptyOperator``: For sources that do not have tests or freshness checks.
    - ``DbtSourceOperator``: For sources that have freshness checks.
    - ``DbtTestOperator``: For sources that have tests.

  This approach aims to create a comprehensive DAG that aligns with dbt documentation, allowing for the rendering of both sources and models for a more detailed visual representation.
  It also ensures that model dependencies do not run if their sources are not fresh, thus preventing the execution of stale or incomplete data.

- **none** (default): When set to ``none``, Cosmos does not automatically render any sources. Note that if node converters are being used for sources, they will still function as intended.

- **with_tests_or_freshness**: When set to ``with_tests_or_freshness``, Cosmos only renders sources that have either tests or freshness checks.

Example:

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig
    from cosmos.constants import SourceRenderingBehavior

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        )
    )


Source Pruning
~~~~~~~~~~~~~~

The ``source_pruning`` is a boolean parameter available in the ``RenderConfig``.
When set to ``True``, it automatically removes (or "prunes") any dbt source nodes from your Airflow DAG that do not have any downstream dependencies within the selected portion of the dbt graph.

This is particularly useful for keeping your DAGs clean and focused, especially in large dbt projects where you might be selecting only a subset of models to run.

By default, this option is set to ``False``.

Example:

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig

    jaffle_shop_pruned = DbtTaskGroup(
        render_config=RenderConfig(
            select=["+customers"],  # select only customers and its parents
            source_pruning=True,
        )
    )

In this example, if the ``jaffle_shop`` project has multiple sources, but only some of them are upstream of the ``customers`` model, Cosmos will only render the necessary sources and prune the rest.


on_warning_callback Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``on_warning_callback`` is a callback parameter available on the ``DbtSourceLocalOperator``. This callback is triggered when a warning occurs during the execution of the ``dbt source freshness`` command. The callback accepts the task context, which includes additional parameters: test_names and test_results

Example:

.. literalinclude:: ../../dev/dags/example_source_rendering.py/
    :language: python
    :start-after: [START cosmos_source_node_example]
    :end-before: [END cosmos_source_node_example]
