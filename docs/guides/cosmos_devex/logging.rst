.. _logging:

Logging
-------

Cosmos allows for a custom logger implementation that adds ``(astronomer-cosmos)`` to each log message.

By default this is not enabled; you can enable it with:

.. code-block:: cfg

    [cosmos]
    rich_logging = True

or

.. code-block:: bash

    export AIRFLOW__COSMOS__RICH_LOGGING=True  

Previous versions of Cosmos had a feature called ``propagate_logs`` to handle issues with Cosmos's previous logging implementation on some systems.
This config option is deprecated.
