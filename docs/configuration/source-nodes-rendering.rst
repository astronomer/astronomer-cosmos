.. _source-nodes-rendering:

Source Nodes Rendering
================

Cosmos by default renders every dbt source as one of the following:

- ``EmptyOperator`` if the source does not have `tests <https://docs.getdbt.com/docs/build/data-tests>`_ or `freshness checks <https://docs.getdbt.com/reference/resource-properties/freshness>`_.
- ``DbtSourceOperator`` which tests the source's freshness.
- ``DbtTestOperator`` which runs the source's tests.

This setup aims to create a DAG that aligns with dbt documentation, rendering both sources and models for enhanced visual representation.
It also prevents model dependencies from running if their sources are not fresh, avoiding the execution of stale data or writing incomplete data to your warehouse.

This can be overridden using the ``source_rendering_behavior`` field in the ``RenderConfig`` object.

Cosmos supports the following source rendering behaviors:

- ``all`` (default): Renders all sources in the dbt project, using the 3 different node types: ``EmptyOperator``, ``DbtSourceOperator`` and ``DbtTestOperator``.
- ``none``: sources won't be rendered using cosmos' default source node rendering. Note: If node converters are being used for sources, they will still function.
- ``with_tests_or_freshness``: renders only sources that have either tests or freshness checks.

Example:

.. code-block:: python

    from cosmos import DbtTaskGroup, RenderConfig
    from cosmos.constants import SourceRenderingBehavior

    jaffle_shop = DbtTaskGroup(
        render_config=RenderConfig(
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        )
    )
