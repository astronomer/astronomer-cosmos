Render Config
================


Cosmos aims to give you control over how your dbt project is rendered as an Airflow DAG or Task Group.
It does this by exposing a ``cosmos.config.RenderConfig`` class that you can use to configure how your DAGs are rendered.

The ``RenderConfig`` class takes the following arguments:

- ``emit_datasets_from``: whether or not to emit Airflow datasets to be used for data-aware scheduling based on if tests are run. Defaults to None
- ``test_behavior``: how to run tests. Defaults to running a model's tests immediately after the model is run. For more information, see the `Testing Behavior <testing-behavior.html>`_ section.
- ``load_method``: how to load your dbt project. See `Parsing Methods <parsing-methods.html>`_ for more information.
- ``select`` and ``exclude``: which models to include or exclude from your DAGs. See `Selecting & Excluding <selecting-excluding.html>`_ for more information.
