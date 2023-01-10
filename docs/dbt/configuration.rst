Configuration
================

Cosmos offers a few different configuration options for how your dbt project is run and structured. This page describes the available options and how to configure them.

Testing
----------------------

By default, Cosmos will add a test after each model. This can be overriden using the ``test_behavior`` field. The options are:

- ``after_each`` (default): turns each model into a task group with two steps: run the model, and run the tests
- ``after_all``: each model becomes a single task, and the tests only run if all models are run successfully
- ``none``: don't include tests

Example:

.. code-block:: python

   from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        # ...
        test_behavior='after_all',
    )


Tags
----------------------

Cosmos allows you to filter by tags using the ``dbt_tags`` parameter. If a model contains any of the tags, it gets included as part of the DAG/Task Group. Otherwise, it doesn't get included (even if rendered models depend on a non-tagged model).

.. note::
    Cosmos currently reads from (1) config calls in the model code and (2) .yml files in the models directory for tags. It does not read from the dbt_project.yml file.

Example:

.. code-block:: python

   from cosmos.providers.dbt import DbtDag

    jaffle_shop = DbtDag(
        # ...
        dbt_tags=['daily'],
    )
