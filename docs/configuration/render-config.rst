Render Config
================


Cosmos aims to give you control over how your dbt project is rendered as an Airflow DAG or Task Group.
It does this by exposing a ``cosmos.config.RenderConfig`` class that you can use to configure how your DAGs are rendered.

The ``RenderConfig`` class takes the following arguments:

- ``emit_datasets``: whether or not to emit Airflow datasets to be used for data-aware scheduling. Defaults to True. Depends on `additional dependencies <lineage.html>`_. If a model in the project has a name containing multibyte characters, the dataset name will be URL-encoded.
- ``test_behavior``: how to run tests. Defaults to running a model's tests immediately after the model is run. For more information, see the `Testing Behavior <testing-behavior.html>`_ section.
- ``load_method``: how to load your dbt project. See `Parsing Methods <parsing-methods.html>`_ for more information.
- ``select`` and ``exclude``: which models to include or exclude from your DAGs. See `Selecting & Excluding <selecting-excluding.html>`_ for more information.
- ``selector``: (new in v1.3) name of a dbt YAML selector to use for DAG parsing. Only supported when using ``load_method=LoadMode.DBT_LS``. See `Selecting & Excluding <selecting-excluding.html>`_ for more information.
- ``dbt_deps``: A Boolean to run dbt deps when using dbt ls for dag parsing. Default True
- ``node_converters``: a dictionary mapping a ``DbtResourceType`` into a callable. Users can control how to render dbt nodes in Airflow. Only supported when using ``load_method=LoadMode.DBT_MANIFEST`` or ``LoadMode.DBT_LS``. Find more information below.
- ``dbt_executable_path``: The path to the dbt executable for dag generation. Defaults to dbt if available on the path.
- ``env_vars``: (available in v1.2.5, use``ProjectConfig.env_vars`` for v1.3.0 onwards) A dictionary of environment variables for rendering. Only supported when using ``load_method=LoadMode.DBT_LS``.
- ``dbt_project_path``: Configures the DBT project location accessible on their airflow controller for DAG rendering - Required when using ``load_method=LoadMode.DBT_LS`` or ``load_method=LoadMode.CUSTOM``
- ``airflow_vars_to_purge_cache``: (new in v1.5) Specify Airflow variables that will affect the ``LoadMode.DBT_LS`` cache. See `Caching <./caching.html>`_ for more information.
- ``source_rendering_behavior``: Determines how source nodes are rendered when using cosmos default source node rendering (ALL, NONE, WITH_TESTS_OR_FRESHNESS). Defaults to "NONE" (since Cosmos 1.6). See `Source Nodes Rendering <./source-nodes-rendering.html>`_ for more information.
- ``normalize_task_id``: A callable that takes a dbt node as input and returns the task ID. This function allows users to set a custom task_id independently of the model name, which can be specified as the task’s display_name. This way, task_id can be modified using a user-defined function, while the model name remains as the task’s display name. The display_name parameter is available in Airflow 2.9 and above. See `Task display name <./task-display-name.html>`_ for more information.
- ``load_method``: how to load your dbt project. See `Parsing Methods <parsing-methods.html>`_ for more information.
- ``should_detach_multiple_parents_tests``: A boolean to control if tests that depend on multiple parents should be run as standalone tasks. See `Parsing Methods <testing-behavior.html>`_ for more information.

Customizing how nodes are rendered (experimental)
-------------------------------------------------

There are circumstances when choosing specific Airflow operators to represent a dbt node is helpful.
An example could be to use an S3 sensor to represent dbt sources or to create custom operators to handle exposures.
Your pipeline may even have specific node types not part of the standard dbt definitions.

The following example illustrates how it is possible to tell Cosmos how to convert two different types of nodes (``source`` and ``exposure``) into Airflow:

.. literalinclude::  ../../dev/dags/example_cosmos_sources.py
    :language: python
    :start-after: [START custom_dbt_nodes]
    :end-before: [END custom_dbt_nodes]

When defining the mapping for a new type that is not part of Cosmos' ``DbtResourceType`` enumeration, users should use the syntax ``DbtResourceType("new-node-type")`` as opposed to ``DbtResourceType.EXISTING_TYPE``. It will dynamically add the new type to the enumeration ``DbtResourceType`` so that Cosmos can parse these dbt nodes and convert them into the Airflow DAG.
