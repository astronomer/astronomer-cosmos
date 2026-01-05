.. _seed-nodes-rendering:

Seed Nodes Rendering
====================

.. note::
    This feature is available since Cosmos 1.13.0.

By default, Cosmos renders and runs all dbt seeds in the project.
You can control this behavior using the ``seed_rendering_behavior`` field in the ``RenderConfig`` object.

Seed Rendering Behavior Options
-------------------------------

- **always** (default):
  When set to ``always``, Cosmos renders and runs all seeds in the dbt project.

- **none**:
  When set to ``none``, Cosmos does not render any seeds. This is useful when seeds are managed outside of the DAG or when seed data has already been loaded.

- **when_seed_changes**:
  When set to ``when_seed_changes``, Cosmos renders the seed task but only executes it if the CSV file has changed since the last successful execution.
  This is useful for optimizing DAG runs by avoiding unnecessary seed reloads.

  The change detection works by:

  1. Computing a SHA256 hash of the seed CSV file at execution time
  2. Comparing the hash with the previously stored hash (stored as an Airflow Variable)
  3. Skipping execution if the hashes match, or running the seed if they differ
  4. Storing the new hash after successful execution

.. warning::
    **Limitations of** ``when_seed_changes``:

    - Only supported with ``ExecutionMode.LOCAL``. Other execution modes (Docker, Kubernetes, etc.) cannot access the seed CSV files from the Airflow worker.
    - **Not compatible with** ``TestBehavior.BUILD``. When using ``TestBehavior.BUILD``, the ``dbt build`` command runs seeds, models, and tests together, so the seed change detection logic is bypassed. Use ``TestBehavior.AFTER_EACH`` or ``TestBehavior.AFTER_ALL`` instead.

Test Behavior for Seeds
-----------------------

Test behavior for seeds is controlled separately via the ``test_behavior`` parameter in ``RenderConfig``, not through ``seed_rendering_behavior``.
This separation of concerns makes it easier to configure seed rendering and test execution independently.

For example:

- To run seeds with tests after each: use ``seed_rendering_behavior=ALWAYS`` with ``test_behavior=AFTER_EACH``
- To run seeds without tests: use ``seed_rendering_behavior=ALWAYS`` with ``test_behavior=NONE``
- To run seeds only when changed, with tests after all: use ``seed_rendering_behavior=WHEN_SEED_CHANGES`` with ``test_behavior=AFTER_ALL``

Examples
--------

Always Render Seeds (Default)
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from cosmos import DbtDag, RenderConfig
    from cosmos.constants import SeedRenderingBehavior

    dag = DbtDag(
        # ... other config ...
        render_config=RenderConfig(
            seed_rendering_behavior=SeedRenderingBehavior.ALWAYS,
        ),
    )

Never Render Seeds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from cosmos import DbtDag, RenderConfig
    from cosmos.constants import SeedRenderingBehavior

    dag = DbtDag(
        # ... other config ...
        render_config=RenderConfig(
            seed_rendering_behavior=SeedRenderingBehavior.NONE,
        ),
    )

Render Seeds Only When Changed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from cosmos import DbtDag, RenderConfig
    from cosmos.constants import SeedRenderingBehavior, TestBehavior

    dag = DbtDag(
        # ... other config ...
        render_config=RenderConfig(
            seed_rendering_behavior=SeedRenderingBehavior.WHEN_SEED_CHANGES,
            # Use AFTER_EACH or AFTER_ALL, not BUILD
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

Seeds Without Tests
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from cosmos import DbtDag, RenderConfig
    from cosmos.constants import SeedRenderingBehavior, TestBehavior

    dag = DbtDag(
        # ... other config ...
        render_config=RenderConfig(
            seed_rendering_behavior=SeedRenderingBehavior.ALWAYS,
            test_behavior=TestBehavior.NONE,
        ),
    )

Full Example DAG
~~~~~~~~~~~~~~~~

.. literalinclude:: ../../dev/dags/example_seed_rendering.py
    :language: python
    :start-after: [START cosmos_seed_always_example]
    :end-before: [END cosmos_seed_always_example]
