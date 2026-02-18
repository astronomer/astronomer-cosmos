.. _selecting-excluding:

Selecting & Excluding
=======================

Cosmos allows you to filter to a subset of your dbt project in each ``DbtDag`` / ``DbtTaskGroup`` using the ``select`` and ``exclude`` parameters in the ``RenderConfig`` class.

    Since Cosmos 1.3, the ``selector`` parameter is available in ``RenderConfig`` when using the ``LoadMode.DBT_LS`` to parse the dbt project into Airflow.

    Since Cosmos 1.13, the ``selector`` parameter is available in ``RenderConfig`` when using the ``LoadMode.DBT_MANIFEST`` to parse the dbt project into Airflow.


Using ``select`` and ``exclude``
--------------------------------

The ``select`` and ``exclude`` parameters are lists, with values like the following:

- ``tag:my_tag``: include/exclude models with the tag ``my_tag``
- ``config.meta.some_key:some_value``: include/exclude models with ``config.meta_some_key: some_value``
- ``config.materialized:table``: include/exclude models with the config ``materialized: table``
- ``path:analytics/tables``: include/exclude models in the ``analytics/tables`` directory. In this example, ``analytics/table`` is a relative path, but absolute paths are also supported.
- ``+node_name+1`` (graph operators): include/exclude the node with name ``node_name``, all its parents, and its first generation of children (`dbt graph selector docs <https://docs.getdbt.com/reference/node-selection/graph-operators>`_)
- ``+/path/to/model_g+`` (graph operators): include/exclude all the nodes in the absolute path ``/path/to/model_g``, their parents and children. Relative paths are also supported.
- ``+tag:nightly`` (graph operators): include/exclude all nodes that have tag ``nightly`` and their parents.
- ``+config.materialized:view`` (graph operators): include/exclude all the nodes that have the materialization ``view`` and their parents
- ``@node_name`` (@ operator): include/exclude the node with name ``node_name``, all its descendants, and all ancestors of those descendants. This is useful in CI environments where you want to build a model and all its descendants, but you need the ancestors of those descendants to exist first.
- ``tag:my_tag,+node_name`` (intersection): include/exclude ``node_name`` and its parents if they have the tag ``my_tag`` (`dbt set operator docs <https://docs.getdbt.com/reference/node-selection/set-operators>`_)
- ``['tag:first_tag', 'tag:second_tag']`` (union): include/exclude nodes that have either ``tag:first_tag`` or ``tag:second_tag``
- ``resource_type:<resource>``: include nodes with the resource type ``seed, snapshot, model, test, source``. For example, ``resource_type:source`` returns only nodes where resource_type == SOURCE
- ``exclude_resource_type:<resource>``: exclude nodes with the resource type ``analysis, exposure, metric, model, saved_query, seed, semantic_model, snapshot, source, test, unit_test``. For example, ``exclude_resource_type:source`` returns only nodes where resource_type != SOURCE
- ``source:my_source``: include/exclude nodes that have the source ``my_source`` and are of resource_type ``source``
- ``source:my_source+``: include/exclude nodes that have the source ``my_source`` and their children
- ``source:my_source.my_table``: include/exclude nodes that have the source ``my_source`` and the table ``my_table``
- ``exposure:my_exposure``: include/exclude nodes that have the exposure ``my_exposure`` and are of resource_type ``exposure``
- ``exposure:+my_exposure``: include/exclude nodes that have the exposure ``my_exposure`` and their parents
- ``package:package_name``: include/exclude all nodes that belong to the given package (e.g. ``package:dbt_artifacts``). Especially useful with ``LoadMode.DBT_MANIFEST`` when excluding installed packages from the DAG. A bare name without a method prefix (e.g. ``dbt_artifacts`` or ``child``) is resolved like dbt: it matches nodes by package name, node name, or path segment (folder name). So ``select=['folder_a']`` or ``exclude=['folder_a']`` includes or excludes all models under a folder named ``folder_a``, including when using ``LoadMode.DBT_MANIFEST``.

.. note::

    If you're using the ``dbt_ls`` parsing method, these arguments are passed directly to the dbt CLI command.

    If you're using the ``dbt_manifest`` parsing method, Cosmos will filter the models in the manifest before creating the DAG. This does not directly use dbt's CLI command, but should include all metadata that dbt would include.

    If you're using the ``custom`` parsing method, Cosmos does not currently read the ``dbt_project.yml`` file. You can still select/exclude models if you're selecting on metadata defined in the model code or ``.yml`` files in the models directory.

Examples:

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:my_tag"],
        )
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["config.schema:prod"],
        )
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["path:analytics/tables"],
        )
    )


.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:include_tag1", "tag:include_tag2"],  # union
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:include_tag1,tag:include_tag2"],  # intersection
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            exclude=["node_name+"],  # node_name and its children
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            exclude=[
                "package:dbt_artifacts"
            ],  # exclude all nodes from dbt_artifacts package (e.g. when using manifest load mode)
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["@my_model"],  # selects my_model, all its descendants,
            # and all ancestors needed to build those descendants
        )
    )

Using ``selector``
--------------------------------
.. note::
    Only currently supported using the ``LoadMode.DBT_LS`` (since Cosmos 1.3) or ``LoadMode.DBT_MANIFEST`` (since Cosmos 1.13).
    If  ``select`` and/or ``exclude`` are used with ``selector``, dbt will ignore the ``select`` and ``exclude`` parameters.

The ``selector`` parameter is a string that references a `dbt YAML selector <https://docs.getdbt.com/reference/node-selection/yaml-selectors>`_ already defined in a dbt project.

Examples:

.. code-block:: python

    from cosmos import DbtDag, RenderConfig, LoadMode

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            selector="my_selector",  # this selector must be defined in your dbt project
            load_method=LoadMode.DBT_LS,
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig, LoadMode

    jaffle_shop = DbtDag(
        project_config=ProjectConfig(
            manifest_path=DBT_ROOT_PATH / "jaffle_shop" / "target" / "manifest.json",
            project_name="jaffle_shop",
        ),
        render_config=RenderConfig(
            selector="nightly_models",  # this selector must be defined in your dbt project
            load_method=LoadMode.DBT_MANIFEST,
        ),
    )
    jaffle_shop_remote = DbtDag(
        project_config=ProjectConfig(
            manifest_path="s3://cosmos-manifest-test/manifest.json",
            manifest_conn_id="aws_s3_conn",
            project_name="jaffle_shop",
        ),
        render_config=RenderConfig(
            selector="nightly_models",  # this selector must be defined in your dbt project
            load_method=LoadMode.DBT_MANIFEST,
        ),
    )

Using ``selector`` with ``LoadMode.DBT_MANIFEST``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since Cosmos 1.13, the ``selector`` parameter is also supported when using the ``LoadMode.DBT_MANIFEST`` parsing method.

When using this combination, Cosmos will read the preprocessed YAML selectors from the manifest file and use them to filter the dbt nodes to include in the Airflow DAG or Task Group.

The YAML selection parser expects the selectors to be defined in the dbt project and will parse the preprocessed ``selectors`` found in the manifest file. Modifying the selector definitions in the manifest file in any way may lead to undefined behavior.
The parser may or may not catch invalid selector definitions if the selectors in the manifest are altered.

The YAML selection parsing logic is based off the spec defined in the `dbt documentation <https://docs.getdbt.com/reference/node-selection/yaml-selectors>`_.
All `graph operators <https://docs.getdbt.com/reference/node-selection/graph-operators>`_ and `set operators <https://docs.getdbt.com/reference/node-selection/set-operators>`_ are supported.
Parsing of the ``default`` and ``indirect_selection`` keywords is not currently supported.

In the event the dbt YAML selector specification changes, Cosmos will attempt to keep up to date with the changes, but there may be a lag between dbt releases and Cosmos releases.
Once a new Cosmos version is released with the updated selector parsing logic, users should update their Cosmos version to ensure compatibility with the latest dbt selector specification.
For subsequent updates to the YAML selector parser, existing YAML selector caches will be invalidated the next time the DAG is parsed.

**Error Handling**

Cosmos distinguishes between two types of errors when parsing YAML selectors:

- **Structural YAML Errors** - These cause immediate failure during manifest parsing:

  - Selector definition is not a dictionary
  - Missing required ``name`` key
  - Missing required ``definition`` key

  These errors indicate malformed YAML structure and will raise a ``CosmosValueError`` immediately when calling ``YamlSelectors.parse()``.

- **Selector Definition Errors** - These are isolated and surfaced when accessing the selector:

  - Unsupported selector methods (e.g., ``method: "state"``, ``method: "package"``)
  - Invalid graph operator configurations (e.g., non-integer depth values)
  - Invalid selector logic (e.g., multiple root keys in a definition)

  These errors are collected during parsing but only raised when you attempt to retrieve the selector using ``get_parsed(selector_name)``.
  This allows the manifest to be loaded successfully even if some selectors have definition errors, enabling you to work with valid selectors while debugging invalid ones.

If a selector has multiple definition errors, they will all be reported together in a formatted error message when accessing the selector.
